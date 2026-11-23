use crate::CrudStore;
use anyhow::Result;
use pioneer_compaction::SourceRef;
use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
#[cfg(any(test, feature = "test-support"))]
use pioneer_crud_compaction::accepted_import::CheckpointImportGraphReadObserver;
use pioneer_crud_compaction::accepted_import::{
    DeliveryCheckpointImportSource, FrozenImportRecord, PreparedFrozenImport,
};
use pioneer_crud_compaction::frozen_lifetime::FrozenReadHold;
use pioneer_crud_compaction::frozen_maintenance;
use std::time::{Duration, Instant};

impl CrudStore {
    pub async fn compaction_begin_frozen_history(
        &self,
        workspace: &str,
        owner_thread: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<()> {
        self.compaction_store()
            .compaction_begin_frozen_history(workspace, owner_thread, descriptor)
            .await
    }

    pub async fn compaction_begin_frozen_history_with_imports(
        &self,
        workspace: &str,
        owner_thread: &str,
        descriptor: &FrozenHistoryRef,
        imports: u64,
        imports_sha256: &str,
    ) -> Result<()> {
        self.compaction_store()
            .compaction_begin_frozen_history_with_imports(
                workspace,
                owner_thread,
                descriptor,
                imports,
                imports_sha256,
            )
            .await
    }

    /// The prepared JSON contains typed references and hashes only. Before DB
    /// admission it is validated and bounded; the transaction revalidates the
    /// manifest owner/readiness and exact existing bytes on an idempotent retry.
    pub async fn compaction_append_frozen_history(
        &self,
        workspace: &str,
        owner_thread: &str,
        manifest: &str,
        start: u64,
        messages: &[FrozenMessageRef],
    ) -> Result<()> {
        self.compaction_store()
            .compaction_append_frozen_history(workspace, owner_thread, manifest, start, messages)
            .await
    }

    /// Ready is published only after the bounded writer has filled all exact
    /// ordinals. The content digest is checked by the caller before this CAS.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn compaction_finish_frozen_history(
        &self,
        workspace: &str,
        owner_thread: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_finish_frozen_history(workspace, owner_thread, descriptor)
            .await
    }

    /// Finish and register the capture handoff before releasing the writer.
    pub async fn compaction_finish_frozen_history_held(
        &self,
        workspace: &str,
        owner: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<crate::FrozenReadHold> {
        self.compaction_store()
            .compaction_finish_frozen_history_held(workspace, owner, descriptor)
            .await
    }

    pub async fn compaction_frozen_history_owner(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<Option<String>> {
        self.compaction_store()
            .compaction_frozen_history_owner(workspace, descriptor)
            .await
    }

    pub async fn compaction_frozen_history_page(
        &self,
        workspace: &str,
        owner_thread: &str,
        manifest: &str,
        start: u64,
    ) -> Result<Vec<FrozenMessageRef>> {
        self.compaction_store()
            .compaction_frozen_history_page(workspace, owner_thread, manifest, start)
            .await
    }

    pub async fn compaction_prepare_frozen_import(
        &self,
        workspace: &str,
        destination: &str,
        delivery: &str,
        acknowledgement: &SourceRef,
        output_ordinal: u64,
        source_thread: &str,
        source: &SourceRef,
    ) -> Result<PreparedFrozenImport> {
        self.compaction_store()
            .compaction_prepare_frozen_import(
                workspace,
                destination,
                delivery,
                acknowledgement,
                output_ordinal,
                source_thread,
                source,
            )
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn compaction_prepare_delivery_checkpoint_imports(
        &self,
        workspace: &str,
        destination: &str,
        delivery: &str,
        acknowledgement: &SourceRef,
        sources: &[DeliveryCheckpointImportSource],
        checkpoint_thread: &str,
        checkpoint: &SourceRef,
    ) -> Result<Vec<PreparedFrozenImport>> {
        self.compaction_store()
            .compaction_prepare_delivery_checkpoint_imports(
                workspace,
                destination,
                delivery,
                acknowledgement,
                sources,
                checkpoint_thread,
                checkpoint,
            )
            .await
    }

    pub async fn compaction_prepare_accepted_import(
        &self,
        workspace: &str,
        destination: &str,
        turn: &str,
        ordinal: u64,
    ) -> Result<PreparedFrozenImport> {
        self.compaction_store()
            .compaction_prepare_accepted_import(workspace, destination, turn, ordinal)
            .await
    }

    pub async fn compaction_prepare_accepted_checkpoint_import(
        &self,
        workspace: &str,
        destination: &str,
        turn: &str,
        ordinal: u64,
        checkpoint_thread: &str,
        checkpoint: &SourceRef,
    ) -> Result<PreparedFrozenImport> {
        self.compaction_store()
            .compaction_prepare_accepted_checkpoint_import(
                workspace,
                destination,
                turn,
                ordinal,
                checkpoint_thread,
                checkpoint,
            )
            .await
    }

    pub async fn compaction_prepare_accepted_checkpoint_imports(
        &self,
        workspace: &str,
        destination: &str,
        turn: &str,
        ordinals: &[u64],
        checkpoint_thread: &str,
        checkpoint: &SourceRef,
    ) -> Result<Vec<PreparedFrozenImport>> {
        self.compaction_store()
            .compaction_prepare_accepted_checkpoint_imports(
                workspace,
                destination,
                turn,
                ordinals,
                checkpoint_thread,
                checkpoint,
            )
            .await
    }

    pub async fn compaction_append_frozen_imports(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
        start: u64,
        imports: &[(u64, PreparedFrozenImport)],
    ) -> Result<()> {
        self.compaction_store()
            .compaction_append_frozen_imports(workspace, owner, manifest, start, imports)
            .await
    }

    pub async fn compaction_frozen_import_state(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
    ) -> Result<Option<(u64, String)>> {
        self.compaction_store()
            .compaction_frozen_import_state(workspace, owner, manifest)
            .await
    }

    pub async fn compaction_frozen_import_page(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
        start: u64,
    ) -> Result<Vec<FrozenImportRecord>> {
        self.compaction_store()
            .compaction_frozen_import_page(workspace, owner, manifest, start)
            .await
    }

    pub async fn compaction_equivalent_frozen_history(
        &self,
        workspace: &str,
        owner: &str,
        descriptor: &FrozenHistoryRef,
        imports: u64,
        digest: &str,
    ) -> Result<Option<FrozenHistoryRef>> {
        self.compaction_store()
            .compaction_equivalent_frozen_history(workspace, owner, descriptor, imports, digest)
            .await
    }

    pub async fn compaction_share_frozen_prefix(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
        messages: &[pioneer_compaction::frozen::FrozenMessageRef],
        imports: &[(u64, PreparedFrozenImport)],
    ) -> Result<(u64, u64)> {
        self.compaction_store()
            .compaction_share_frozen_prefix(workspace, owner, manifest, messages, imports)
            .await
    }

    pub async fn compact_frozen_storage_quantum(
        &self,
        progress: &mut crate::FrozenStorageLifetimeProgress,
    ) -> Result<bool> {
        self.compaction_store()
            .compact_frozen_storage_quantum(&mut progress.0)
            .await
    }

    pub async fn compaction_acquire_frozen_history(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<FrozenReadHold> {
        self.compaction_store()
            .compaction_acquire_frozen_history(workspace, descriptor)
            .await
    }
}

#[cfg(any(test, feature = "test-support"))]
pub fn observe_checkpoint_import_graph_reads(
    store: &crate::CrudStore,
    workspace: &str,
) -> CheckpointImportGraphReadObserver {
    pioneer_crud_compaction::accepted_import::observe_checkpoint_import_graph_reads(
        &store.connection,
        workspace,
    )
}

// Share the configured reader holds with frozen storage maintenance.

#[derive(Default)]
pub struct FrozenStorageLifetimeProgress(frozen_maintenance::FrozenStorageLifetimeProgress);

impl FrozenStorageLifetimeProgress {
    pub fn restart_discovery(&mut self) {
        self.0.restart_discovery();
    }

    pub async fn restart_for_changes(
        &mut self,
        store: &CrudStore,
        keys: Option<Vec<String>>,
    ) -> Result<bool> {
        self.0
            .restart_for_changes(&store.compaction_store(), keys)
            .await
    }

    pub fn recheck_released_holds(&mut self) {
        self.0.recheck_released_holds();
    }

    pub async fn next_retry_deadline(&self, store: &CrudStore) -> Result<Option<i64>> {
        self.0.next_retry_deadline(&store.compaction_store()).await
    }

    pub async fn quantum(&mut self, store: &CrudStore) -> Result<bool> {
        self.0.quantum(&store.compaction_store()).await
    }

    pub fn revisit_completed_cleanup(&mut self, now: Instant, interval: Duration) {
        self.0.revisit_completed_cleanup(now, interval);
    }
}
