use crate::CrudStore;
use anyhow::Result;
use pioneer_compaction::{Checkpoint, OperationSnapshot, SourceRef};
use pioneer_crud_compaction::checkpoint_apply::{CommitOutcome, SourceAssertion};
use pioneer_crud_compaction::checkpoint_projection::CheckpointMetadata;
use pioneer_crud_compaction::checkpoint_proofs::CheckpointEdges;
use pioneer_crud_compaction::operations::OperationRecord;

impl CrudStore {
    pub async fn compaction_admit(
        &self,
        workspace: &str,
        thread: &str,
        snapshot: &OperationSnapshot,
    ) -> Result<OperationRecord> {
        self.compaction_store()
            .compaction_admit(workspace, thread, snapshot)
            .await
    }

    pub async fn compaction_admit_for_turn(
        &self,
        workspace: &str,
        thread: &str,
        snapshot: &OperationSnapshot,
        execution_turn: Option<&str>,
    ) -> Result<OperationRecord> {
        self.compaction_store()
            .compaction_admit_for_turn(workspace, thread, snapshot, execution_turn)
            .await
    }

    pub async fn compaction_operation_for_plan(
        &self,
        workspace: &str,
        thread: &str,
        owner: &str,
        fingerprint: &str,
    ) -> Result<Option<OperationRecord>> {
        self.compaction_store()
            .compaction_operation_for_plan(workspace, thread, owner, fingerprint)
            .await
    }

    pub async fn compaction_invalid_completion_progress(
        &self,
        workspace: &str,
        thread: &str,
        owner: &str,
    ) -> Result<Option<OperationRecord>> {
        self.compaction_store()
            .compaction_invalid_completion_progress(workspace, thread, owner)
            .await
    }

    pub async fn compaction_operation(&self, id: &str) -> Result<Option<OperationRecord>> {
        self.compaction_store().compaction_operation(id).await
    }

    /// This is the only admission of a provider attempt. Counters survive restart.
    pub async fn compaction_claim_attempt(
        &self,
        id: &str,
        expected_attempts: i64,
        now_ms: i64,
        transient_retry: bool,
        correction: bool,
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_claim_attempt(id, expected_attempts, now_ms, transient_retry, correction)
            .await
    }

    pub async fn compaction_finish(&self, id: &str, status: &str, outcome: &str) -> Result<()> {
        self.compaction_store()
            .compaction_finish(id, status, outcome)
            .await
    }

    /// Saves complete intermediate results without publishing a working pointer.
    pub async fn compaction_save_candidate(
        &self,
        checkpoint: &Checkpoint,
        portion: i64,
    ) -> Result<()> {
        self.compaction_store()
            .compaction_save_candidate(checkpoint, portion)
            .await
    }

    /// Read bounded historical coverage metadata without loading summary text.
    pub async fn compaction_checkpoint_edges(&self, id: &str) -> Result<Option<CheckpointEdges>> {
        self.compaction_store()
            .compaction_checkpoint_edges(id)
            .await
    }

    pub async fn compaction_checkpoint(&self, id: &str) -> Result<Option<Checkpoint>> {
        self.compaction_store().compaction_checkpoint(id).await
    }

    /// Projection fields without coverage. Callers separately validate the
    /// published root identity and accepted context boundary.
    pub async fn compaction_checkpoint_body(
        &self,
        id: &str,
    ) -> Result<Option<pioneer_crud_compaction::checkpoint_projection::CheckpointBody>> {
        self.compaction_store().compaction_checkpoint_body(id).await
    }

    pub async fn compaction_checkpoint_metadata(
        &self,
        id: &str,
    ) -> Result<Option<CheckpointMetadata>> {
        self.compaction_store()
            .compaction_checkpoint_metadata(id)
            .await
    }

    /// Atomic CAS. Appends do not invalidate selected sources; edits, Stop and another head do.
    pub async fn compaction_apply(
        &self,
        checkpoint: &Checkpoint,
        expected_head: Option<&str>,
        assertions: &[SourceAssertion],
    ) -> Result<CommitOutcome> {
        self.compaction_store()
            .compaction_apply(checkpoint, expected_head, assertions)
            .await
    }

    pub async fn compaction_checkpoint_source(
        &self,
        workspace: &str,
        thread: &str,
        checkpoint: &str,
    ) -> Result<Option<SourceRef>> {
        self.compaction_store()
            .compaction_checkpoint_source(workspace, thread, checkpoint)
            .await
    }
}
