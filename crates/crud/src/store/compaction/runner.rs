use crate::CrudStore;
use anyhow::Result;
use pioneer_compaction::frozen::FrozenHistoryRef;
use pioneer_compaction::runner::RunnerState;
use pioneer_compaction::{Checkpoint, ModelBudget};
use pioneer_crud_compaction::checkpoint_apply::CommitOutcome;
#[cfg(any(test, feature = "test-support"))]
use pioneer_crud_compaction::publication_test_hook::{
    PublicationTestHookHandle, PublicationTestPause,
};
use pioneer_crud_compaction::runner_repository::{ManifestEntry, RunnerPlanRecord};

impl CrudStore {
    pub async fn compaction_prepare_runner(
        &self,
        operation: &str,
        budget: &ModelBudget,
        source_count: u64,
        reference_count: u64,
    ) -> Result<()> {
        self.compaction_store()
            .compaction_prepare_runner(operation, budget, source_count, reference_count)
            .await
    }

    pub async fn compaction_runner_plan(
        &self,
        operation: &str,
    ) -> Result<Option<RunnerPlanRecord>> {
        self.compaction_store()
            .compaction_runner_plan(operation)
            .await
    }

    pub async fn compaction_append_manifest(
        &self,
        operation: &str,
        entries: &[ManifestEntry],
    ) -> Result<()> {
        self.compaction_store()
            .compaction_append_manifest(operation, entries)
            .await
    }

    pub async fn compaction_manifest_sources_current(&self, operation: &str) -> Result<bool> {
        self.compaction_store()
            .compaction_manifest_sources_current(operation)
            .await
    }

    pub async fn compaction_activate_runner(
        &self,
        operation: &str,
        initial: &RunnerState,
    ) -> Result<()> {
        self.compaction_store()
            .compaction_activate_runner(operation, initial)
            .await
    }

    pub async fn compaction_runner_state(&self, operation: &str) -> Result<Option<RunnerState>> {
        self.compaction_store()
            .compaction_runner_state(operation)
            .await
    }

    /// Complete the state record after a control-plane terminal fence. No
    /// attempt can advance past that fence. Preparation reads one bounded row;
    /// the write revalidates its generation and durable terminal classification.
    pub async fn compaction_reconcile_runner_state(
        &self,
        operation: &str,
    ) -> Result<Option<RunnerState>> {
        self.compaction_store()
            .compaction_reconcile_runner_state(operation)
            .await
    }

    pub async fn compaction_manifest_page(
        &self,
        operation: &str,
        reference_only: bool,
        unit: u64,
        source_offset: u32,
    ) -> Result<Vec<ManifestEntry>> {
        self.compaction_store()
            .compaction_manifest_page(operation, reference_only, unit, source_offset)
            .await
    }

    /// Preparation uses immutable values only. Generation, counters, ownership,
    /// deadline and running status are revalidated atomically with candidate save.
    pub async fn compaction_runner_transition(
        &self,
        operation: &str,
        expected: u64,
        next: &RunnerState,
        candidate: Option<&Checkpoint>,
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_runner_transition(operation, expected, next, candidate)
            .await
    }

    /// Validate the admitted manifest in one reader snapshot, then compare its
    /// compact fence and publish the candidate in one short writer transaction.
    pub async fn compaction_apply_runner(
        &self,
        operation: &str,
        state: &RunnerState,
        expected_head: Option<&str>,
    ) -> Result<CommitOutcome> {
        self.compaction_store()
            .compaction_apply_runner(operation, state, expected_head)
            .await
    }

    pub async fn compaction_bound_source_projection(
        &self,
        operation: &str,
    ) -> Result<Option<FrozenHistoryRef>> {
        self.compaction_store()
            .compaction_bound_source_projection(operation)
            .await
    }

    /// Serialization is outside writer capacity. The writer revalidates the
    /// ready manifest and exact execution/TaskRun snapshot before storing its
    /// identity. This immutable binding also survives operation recovery.
    pub async fn compaction_bind_source_projection(
        &self,
        operation: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<()> {
        self.compaction_store()
            .compaction_bind_source_projection(operation, descriptor)
            .await
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn arm_publication_test_hook(
    store: &CrudStore,
    operation: &str,
    phase: PublicationTestPause,
) -> PublicationTestHookHandle {
    pioneer_crud_compaction::publication_test_hook::arm_publication_test_hook(
        &store.connection,
        operation,
        phase,
    )
}

#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub async fn trigger_publication_test_hook(
    store: &CrudStore,
    operation: &str,
    phase: PublicationTestPause,
) {
    pioneer_crud_compaction::publication_test_hook::trigger_publication_test_hook(
        &store.connection,
        operation,
        phase,
    )
    .await;
}
