//! Public types and test hooks for compaction.

#[cfg(any(test, feature = "test-support"))]
pub use crate::store::compaction::frozen_history::observe_checkpoint_import_graph_reads;
#[cfg(any(test, feature = "test-support"))]
pub use crate::store::compaction::runner::{
    arm_publication_test_hook, trigger_publication_test_hook,
};
pub use pioneer_crud_compaction::CHECKPOINT_SOURCE_LIMIT;
#[cfg(any(test, feature = "test-support"))]
pub use pioneer_crud_compaction::accepted_import::CheckpointImportGraphReadObserver;
pub use pioneer_crud_compaction::accepted_import::{
    DeliveryCheckpointImportSource, EMPTY_FROZEN_IMPORT_SHA256, FROZEN_IMPORT_PAGE_BYTES,
    FrozenImportRecord, PreparedFrozenImport, PreviewArchiveProjection, frozen_import_identity,
};
pub use pioneer_crud_compaction::background::CompletedHistoryCheck;
pub use pioneer_crud_compaction::canonical_history::{
    CanonicalFragment, PagedSource, SourcePage, SourceRecord,
};
pub use pioneer_crud_compaction::checkpoint_apply::{CommitOutcome, SourceAssertion};
pub use pioneer_crud_compaction::checkpoint_projection::{CheckpointBody, CheckpointMetadata};
pub use pioneer_crud_compaction::checkpoint_proofs::{
    CheckpointEdges, HistoricalEventInputEvidence, HistoricalReplayAlias, HistoricalSourceRef,
};
pub use pioneer_crud_compaction::history::{
    AcceptedTaskBasis, ChildHistoryParent, HistoryCausalBoundary, HistoryReadFence,
    HistoryTurnBoundary, TaskInputCopyAlias, event_projection_metadata,
};
pub use pioneer_crud_compaction::history_check_result::{
    HistoryCheckDiagnostic, HistoryCheckOutcome,
};
pub use pioneer_crud_compaction::lifecycle::PreparedCompactionLifecycle;
pub use pioneer_crud_compaction::lifecycle_pending::{
    CompactionLifecycleCandidate, CompactionLifecycleClaim, CompactionLifecycleStorageError,
};
pub use pioneer_crud_compaction::operations::OperationRecord;
#[cfg(any(test, feature = "test-support"))]
pub use pioneer_crud_compaction::publication_test_hook::{
    PublicationTestHookHandle, PublicationTestPause,
};
pub use pioneer_crud_compaction::runner_repository::{ManifestEntry, RunnerPlanRecord};
pub use pioneer_crud_compaction::runner_writes::CanonicalSource;
pub use pioneer_crud_compaction::source_revision::HistoricalEventProjection;
pub use pioneer_crud_compaction::task_output::{
    DeliveredTaskOutputCursor, DeliveredTaskOutputEvent, DeliveredTaskOutputPage,
    DeliveredTaskOutputRef, TaskDeliveryOutputSnapshot, TaskOutputSnapshot,
};
pub use pioneer_crud_compaction::{SOURCE_PAGE_BYTES, SOURCE_PAGE_ROWS};
pub use pioneer_crud_conversations::context_cleanup::{
    MODEL_HISTORY_CONTEXT_SOURCES, model_history_context_source,
};

// Preserve the existing top-level paths for these two compaction types.
pub(super) mod root_exports {
    /// Private per-worker lifetime reconciliation state; constructing it does not activate cleanup.
    pub use crate::store::compaction::frozen_history::FrozenStorageLifetimeProgress;

    pub use pioneer_crud_compaction::frozen_lifetime::FrozenReadHold;
}
