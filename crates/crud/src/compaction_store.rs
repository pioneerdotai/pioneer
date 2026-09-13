//! CrudStore facade; persistence operations live in repositories.
use crate::compaction::{
    AcceptedTaskBasis, CanonicalFragment, CanonicalSource, CheckpointEdges, CommitOutcome,
    CompactionLifecycleRecovery, CompletedHistoryCheck, DeliveredTaskOutputPage,
    FrozenImportRecord, HistoryCausalBoundary, HistoryReadFence, HistoryTurnBoundary,
    ManifestEntry, OperationRecord, PagedSource, PreparedFrozenImport, RunnerPlanRecord,
    SourceAssertion, SourcePage, TaskDeliveryOutputSnapshot, TaskOutputSnapshot,
};
use crate::{CanonicalTurnEventPayload, CrudStore, repositories};
use anyhow::Result;
use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
use pioneer_compaction::runner::RunnerState;
use pioneer_compaction::{Checkpoint, ModelBudget, OperationSnapshot, SourceRef};

impl CrudStore {
    /// Pin append-only discovery to a bounded high water mark. Edits remain
    /// governed by each source revision, not by this sequence boundary.
    pub async fn compaction_source_high_water(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
    ) -> Result<i64> {
        repositories::compaction::compaction_source_high_water(
            &self.connection,
            workspace,
            thread,
            turn,
            kind,
        )
        .await
    }
    /// Scope is inherited from this handle. Background callers must use maintenance.
    /// Metadata discovery is bounded; payload parsing/hashing follows released reads.
    pub async fn compaction_source_page(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
        after: i64,
    ) -> Result<SourcePage> {
        repositories::compaction::compaction_source_page(self, workspace, thread, turn, kind, after)
            .await
    }
    /// Discover versioned identities without materializing already covered
    /// payloads. The projection fetches only its uncovered sources afterward.
    pub async fn compaction_source_metadata_page(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
        after: i64,
    ) -> Result<SourcePage> {
        repositories::compaction::compaction_source_metadata_page(
            self, workspace, thread, turn, kind, after,
        )
        .await
    }
    pub async fn compaction_source_metadata_page_at_fence(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
        after: i64,
        capture_order: i64,
    ) -> Result<SourcePage> {
        repositories::compaction::compaction_source_metadata_page_at_fence(
            self,
            workspace,
            thread,
            turn,
            kind,
            after,
            capture_order,
        )
        .await
    }
    pub(crate) async fn compaction_source_page_inner(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
        after: i64,
        include_payload: bool,
        capture_order: i64,
    ) -> Result<SourcePage> {
        repositories::compaction::compaction_source_page_inner(
            &self.connection,
            workspace,
            thread,
            turn,
            kind,
            after,
            include_payload,
            capture_order,
        )
        .await
    }
    pub async fn compaction_admit(
        &self,
        workspace: &str,
        thread: &str,
        snapshot: &OperationSnapshot,
    ) -> Result<OperationRecord> {
        repositories::compaction::compaction_admit(self, workspace, thread, snapshot).await
    }
    pub async fn compaction_admit_for_turn(
        &self,
        workspace: &str,
        thread: &str,
        snapshot: &OperationSnapshot,
        execution_turn: Option<&str>,
    ) -> Result<OperationRecord> {
        repositories::compaction::compaction_admit_for_turn(
            self,
            workspace,
            thread,
            snapshot,
            execution_turn,
        )
        .await
    }
    pub async fn compaction_operation_for_plan(
        &self,
        workspace: &str,
        thread: &str,
        owner: &str,
        fingerprint: &str,
    ) -> Result<Option<OperationRecord>> {
        repositories::compaction::compaction_operation_for_plan(
            &self.connection,
            workspace,
            thread,
            owner,
            fingerprint,
        )
        .await
    }
    pub async fn compaction_operation(&self, id: &str) -> Result<Option<OperationRecord>> {
        repositories::compaction::compaction_operation(&self.connection, id).await
    }
    /// Validate a bounded batch of exact identities after preparing its JSON
    /// outside reader capacity. An edited/deleted source or stale summary may
    /// never pass an otherwise-fitting native preflight as current history.
    pub async fn compaction_sources_current(
        &self,
        workspace: &str,
        thread: &str,
        sources: &[SourceRef],
    ) -> Result<bool> {
        repositories::compaction::compaction_sources_current(
            &self.connection,
            workspace,
            thread,
            sources,
        )
        .await
    }
    /// Resolve only the storage scope of one exact current revision. Used to
    /// validate transitive checkpoint coverage before any source body is read.
    /// The caller still checks the returned thread against accepted scopes.
    pub async fn compaction_reference_thread(
        &self,
        workspace: &str,
        source: &SourceRef,
    ) -> Result<Option<String>> {
        repositories::compaction::compaction_reference_thread(&self.connection, workspace, source)
            .await
    }
    pub async fn compaction_head(&self, owner: &str) -> Result<Option<String>> {
        repositories::compaction::compaction_head(&self.connection, owner).await
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
        repositories::compaction::compaction_claim_attempt(
            &self.connection,
            id,
            expected_attempts,
            now_ms,
            transient_retry,
            correction,
        )
        .await
    }
    pub async fn compaction_finish(&self, id: &str, status: &str, outcome: &str) -> Result<()> {
        repositories::compaction::compaction_finish(&self.connection, id, status, outcome).await
    }
    /// Saves complete intermediate results without publishing a working pointer.
    pub async fn compaction_save_candidate(
        &self,
        checkpoint: &Checkpoint,
        portion: i64,
    ) -> Result<()> {
        repositories::compaction::compaction_save_candidate(self, checkpoint, portion).await
    }
    /// Coverage discovery must not read summary text before source authorization.
    pub async fn compaction_checkpoint_edges(&self, id: &str) -> Result<Option<CheckpointEdges>> {
        repositories::compaction::compaction_checkpoint_edges(&self.connection, id).await
    }
    pub async fn compaction_checkpoint(&self, id: &str) -> Result<Option<Checkpoint>> {
        repositories::compaction::compaction_checkpoint(&self.connection, id).await
    }
    /// Atomic CAS. Appends do not invalidate selected sources; edits, Stop and another head do.
    pub async fn compaction_apply(
        &self,
        checkpoint: &Checkpoint,
        expected_head: Option<&str>,
        assertions: &[SourceAssertion],
    ) -> Result<CommitOutcome> {
        repositories::compaction::compaction_apply(self, checkpoint, expected_head, assertions)
            .await
    }
    /// Bounded Unicode-safe reads of very large canonical payloads. Revision is
    /// maintained atomically by source-table triggers, including deletes/reinserts.
    /// The same revision must be supplied for every subsequent fragment.
    pub async fn compaction_source_fragment(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        id: &str,
        expected_revision: Option<&str>,
        character_offset: u64,
    ) -> Result<Option<CanonicalFragment>> {
        repositories::compaction::compaction_source_fragment(
            self,
            workspace,
            thread,
            turn,
            id,
            expected_revision,
            character_offset,
        )
        .await
    }
    pub(crate) async fn compaction_payload_fragment(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        id: &str,
        expected_revision: Option<&str>,
        character_offset: u64,
        kind: CanonicalSource,
    ) -> Result<Option<CanonicalFragment>> {
        repositories::compaction::compaction_payload_fragment(
            &self.connection,
            workspace,
            thread,
            turn,
            id,
            expected_revision,
            character_offset,
            kind,
        )
        .await
    }
    /// Resolve a tool item to its existing full canonical record without exposing
    /// another workspace/thread. Authorization remains the caller's mandatory gate.
    pub async fn compaction_tool_result_id(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<String>> {
        repositories::compaction::compaction_tool_result_id(
            &self.connection,
            workspace,
            thread,
            turn,
            item,
        )
        .await
    }
    /// Prefer an already retained full shell source. Other tools use their canonical result.
    /// Both lookups and fragments retain the same workspace/thread/turn scope.
    pub async fn compaction_tool_result_fragment(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
        expected_revision: Option<&str>,
        character_offset: u64,
    ) -> Result<Option<CanonicalFragment>> {
        repositories::compaction::compaction_tool_result_fragment(
            self,
            workspace,
            thread,
            turn,
            item,
            expected_revision,
            character_offset,
        )
        .await
    }
    pub async fn compaction_reference_fragment(
        &self,
        workspace: &str,
        thread: &str,
        reference: &SourceRef,
        character_offset: u64,
    ) -> Result<Option<CanonicalFragment>> {
        repositories::compaction::compaction_reference_fragment(
            self,
            workspace,
            thread,
            reference,
            character_offset,
        )
        .await
    }
    pub async fn compaction_projection_version(
        &self,
        workspace: &str,
        thread: &str,
    ) -> Result<u64> {
        repositories::compaction::compaction_projection_version(&self.connection, workspace, thread)
            .await
    }
    pub async fn compaction_checkpoint_source(
        &self,
        workspace: &str,
        thread: &str,
        checkpoint: &str,
    ) -> Result<Option<SourceRef>> {
        repositories::compaction::compaction_checkpoint_source(
            &self.connection,
            workspace,
            thread,
            checkpoint,
        )
        .await
    }
    /// Resolve a runtime locator only after its durable append was acknowledged.
    /// Reads metadata, never a full result, and cannot cross the execution scope.
    pub async fn compaction_context_reference_for_item(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
        source: &str,
    ) -> Result<Option<SourceRef>> {
        repositories::compaction::compaction_context_reference_for_item(
            &self.connection,
            workspace,
            thread,
            turn,
            item,
            source,
        )
        .await
    }
    pub async fn compaction_tool_item_reference(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<SourceRef>> {
        repositories::compaction::compaction_tool_item_reference(
            &self.connection,
            workspace,
            thread,
            turn,
            item,
        )
        .await
    }
    /// Resolve a saved shell result or frozen provider representation to its tool
    /// item. Read identity metadata only and require the exact live revision.
    pub async fn compaction_replay_item_id(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
    ) -> Result<Option<String>> {
        repositories::compaction::compaction_replay_item_id(
            &self.connection,
            workspace,
            thread,
            source,
        )
        .await
    }
    pub async fn compaction_item_reference(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<SourceRef>> {
        repositories::compaction::compaction_item_reference(
            &self.connection,
            workspace,
            thread,
            turn,
            item,
        )
        .await
    }
    pub async fn compaction_enqueue_native_history_check(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        descriptor: &str,
    ) -> Result<()> {
        repositories::compaction::background::compaction_enqueue_native_history_check(
            &self.connection,
            workspace,
            thread,
            turn,
            descriptor,
        )
        .await
    }
    /// Reconcile lost terminal publications and abandoned deadline/Stop states
    /// using bounded metadata. This scanner never admits a service generation.
    pub async fn compaction_lifecycle_recovery(
        &self,
        now_ms: u64,
        after: &str,
    ) -> Result<Vec<CompactionLifecycleRecovery>> {
        repositories::compaction::background::compaction_lifecycle_recovery(
            &self.connection,
            now_ms,
            after,
        )
        .await
    }
    /// A newer execution invalidates an optional older check. Inspect one
    /// locator at a time rather than bulk-updating a thread's retained history.
    pub async fn compaction_history_check_is_current(&self, turn: &str) -> Result<bool> {
        repositories::compaction::background::compaction_history_check_is_current(
            &self.connection,
            turn,
        )
        .await
    }
    /// One bounded metadata page; caller releases reader capacity before decoding.
    pub async fn compaction_pending_history_checks(&self) -> Result<Vec<CompletedHistoryCheck>> {
        repositories::compaction::background::compaction_pending_history_checks(&self.connection)
            .await
    }
    /// Persist the captured settings/deadline before service admission; restart
    /// reuses this exact descriptor. It contains model metadata, never messages.
    pub async fn compaction_capture_history_check(
        &self,
        turn: &str,
        descriptor: &str,
    ) -> Result<Option<String>> {
        repositories::compaction::background::compaction_capture_history_check(
            &self.connection,
            turn,
            descriptor,
        )
        .await
    }
    pub async fn compaction_finish_history_check(&self, turn: &str, outcome: &str) -> Result<()> {
        repositories::compaction::background::compaction_finish_history_check(
            &self.connection,
            turn,
            outcome,
        )
        .await
    }
    pub async fn compaction_begin_frozen_history(
        &self,
        workspace: &str,
        owner_thread: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<()> {
        repositories::compaction::frozen::compaction_begin_frozen_history(
            self,
            workspace,
            owner_thread,
            descriptor,
        )
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
        repositories::compaction::frozen::compaction_begin_frozen_history_with_imports(
            &self.connection,
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
        repositories::compaction::frozen::compaction_append_frozen_history(
            self,
            workspace,
            owner_thread,
            manifest,
            start,
            messages,
        )
        .await
    }
    /// Ready is published only after the bounded writer has filled all exact
    /// ordinals. The content digest is checked by the caller before this CAS.
    pub async fn compaction_finish_frozen_history(
        &self,
        workspace: &str,
        owner_thread: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<bool> {
        repositories::compaction::frozen::compaction_finish_frozen_history(
            &self.connection,
            workspace,
            owner_thread,
            descriptor,
        )
        .await
    }
    pub async fn compaction_frozen_history_owner(
        &self,
        workspace: &str,
        descriptor: &FrozenHistoryRef,
    ) -> Result<Option<String>> {
        repositories::compaction::frozen::compaction_frozen_history_owner(
            &self.connection,
            workspace,
            descriptor,
        )
        .await
    }
    pub async fn compaction_frozen_history_page(
        &self,
        workspace: &str,
        owner_thread: &str,
        manifest: &str,
        start: u64,
    ) -> Result<Vec<FrozenMessageRef>> {
        repositories::compaction::frozen::compaction_frozen_history_page(
            &self.connection,
            workspace,
            owner_thread,
            manifest,
            start,
        )
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
        repositories::compaction::frozen_import::compaction_prepare_frozen_import(
            self,
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
    pub async fn compaction_append_frozen_imports(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
        start: u64,
        imports: &[(u64, PreparedFrozenImport)],
    ) -> Result<()> {
        repositories::compaction::frozen_import::compaction_append_frozen_imports(
            self, workspace, owner, manifest, start, imports,
        )
        .await
    }
    pub async fn compaction_frozen_import_state(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
    ) -> Result<Option<(u64, String)>> {
        repositories::compaction::frozen_import::compaction_frozen_import_state(
            &self.connection,
            workspace,
            owner,
            manifest,
        )
        .await
    }
    pub async fn compaction_frozen_import_page(
        &self,
        workspace: &str,
        owner: &str,
        manifest: &str,
        start: u64,
    ) -> Result<Vec<FrozenImportRecord>> {
        repositories::compaction::frozen_import::compaction_frozen_import_page(
            &self.connection,
            workspace,
            owner,
            manifest,
            start,
        )
        .await
    }
    pub async fn compaction_turn_is_completed(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<bool> {
        repositories::compaction::history::compaction_turn_is_completed(
            &self.connection,
            workspace,
            thread,
            turn,
        )
        .await
    }
    /// Metadata locator for an already accepted legacy array. Payload remains
    /// in its original immutable TaskRun snapshot, read through source fragments.
    pub async fn compaction_legacy_task_basis_source(
        &self,
        workspace: &str,
        parent: &str,
        run: &str,
    ) -> Result<Option<SourceRef>> {
        repositories::compaction::history::compaction_legacy_task_basis_source(
            &self.connection,
            workspace,
            parent,
            run,
        )
        .await
    }
    /// The parent basis admitted for this exact child execution. Attachment is
    /// deliberately irrelevant: it controls lifecycle/hooks, not history scope.
    /// Read only identity metadata, never the snapshot transcript or Task body.
    pub async fn compaction_task_basis_thread(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<String>> {
        repositories::compaction::history::compaction_task_basis_thread(
            &self.connection,
            workspace,
            thread,
            turn,
        )
        .await
    }
    /// For a destination without a creator execution, select the most recent
    /// admitted basis whose actual input existed at the shared capture fence.
    /// This reads relationship metadata, never a newer ancestor transcript.
    pub async fn compaction_latest_task_basis_turn(
        &self,
        workspace: &str,
        thread: &str,
        fence: &HistoryReadFence,
    ) -> Result<Option<String>> {
        repositories::compaction::history::compaction_latest_task_basis_turn(
            &self.connection,
            workspace,
            thread,
            fence,
        )
        .await
    }
    /// Read the already accepted TaskRun basis in byte-bounded fragments.
    /// Each fragment repeats the exact execution/lineage/workspace predicate;
    /// deletion or reparenting cannot yield a partially authorized transcript.
    /// Snapshot rows are immutable (insert-if-absent); decoding is outside DB
    /// capacity. Legacy arrays retain their original bytes until migration.
    pub async fn compaction_task_basis_snapshot(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<Option<AcceptedTaskBasis>> {
        repositories::compaction::history::compaction_task_basis_snapshot(
            &self.connection,
            workspace,
            thread,
            turn,
        )
        .await
    }
    /// Refresh a stale metadata cache after decoding a scoped canonical source
    /// outside database capacity. The revision CAS prevents a later event edit
    /// from being labelled using the earlier typed payload.
    pub async fn compaction_record_event_projection(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
        event: &crate::CanonicalTurnEventPayload,
    ) -> Result<bool> {
        repositories::compaction::history::compaction_record_event_projection(
            &self.connection,
            workspace,
            thread,
            source,
            event,
        )
        .await
    }
    /// Exact command/outcome relationship for a canonical delivered result.
    /// This is a point metadata query: neither matching text nor a delivered
    /// status alone establishes an alias. Frozen manifests preserve this link.
    pub async fn compaction_task_delivery_command(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
    ) -> Result<Option<String>> {
        repositories::compaction::history::compaction_task_delivery_command(
            self, workspace, thread, source,
        )
        .await
    }
    pub(crate) async fn compaction_failed_delivery_command(
        &self,
        workspace: &str,
        thread: &str,
        command: Option<&str>,
        source: Option<&SourceRef>,
        event_fence: i64,
    ) -> Result<Option<String>> {
        repositories::compaction::history::compaction_failed_delivery_command(
            &self.connection,
            workspace,
            thread,
            command,
            source,
            event_fence,
        )
        .await
    }
    /// Bounded relationship metadata for a single selected turn. A Task status
    /// alone does not close its command: the identified outcome event must
    /// already exist below the same event fence. This grants no child-history
    /// access and reads no Task result or event payload.
    pub async fn compaction_history_causal_boundary(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        fence: &HistoryReadFence,
    ) -> Result<HistoryCausalBoundary> {
        repositories::compaction::history::compaction_history_causal_boundary(
            self, workspace, thread, turn, fence,
        )
        .await
    }
    /// One read fixes the append boundary before enumerating any turns. Source
    /// bounds use retained metadata with explicit insertion order; deleting or
    /// vacuuming canonical rows cannot change that order. MAX uses indexes and
    /// reads no payload. Every subsequent discovery checks its workspace scope.
    pub async fn compaction_history_read_fence(&self) -> Result<HistoryReadFence> {
        repositories::compaction::history::compaction_history_read_fence(&self.connection).await
    }
    /// Discover at most 128 metadata rows, including active turns. Eligibility
    /// is determined from complete canonical rounds/events below the captured
    /// fence, never by a later mutable terminal status alone.
    pub async fn compaction_history_turn_page(
        &self,
        workspace: &str,
        thread: &str,
        after: &str,
        fence: &HistoryReadFence,
    ) -> Result<Vec<HistoryTurnBoundary>> {
        repositories::compaction::history::compaction_history_turn_page(
            &self.connection,
            workspace,
            thread,
            after,
            fence,
        )
        .await
    }
    /// Constant-size control-plane fence. The owning runtime awaits this write
    /// before cancelling service work. It survives worker loss without changing
    /// the completed user Turn or granting a new compaction attempt.
    pub async fn compaction_stop_execution(
        &self,
        workspace: &str,
        thread: &str,
        owner: &str,
        turn: &str,
    ) -> Result<()> {
        repositories::compaction::lifecycle::compaction_stop_execution(
            self, workspace, thread, owner, turn,
        )
        .await
    }
    pub async fn compaction_materialize_lifecycle(
        &self,
        operation: &str,
        generation: u64,
        event: CanonicalTurnEventPayload,
        timestamp_secs: i64,
    ) -> Result<()> {
        repositories::compaction::lifecycle::compaction_materialize_lifecycle(
            self,
            operation,
            generation,
            event,
            timestamp_secs,
        )
        .await
    }
    /// Manifest admission contains metadata only and resumes in bounded batches.
    /// No provider may run until activate_runner has checked the whole manifest.
    /// The execution turn is an immutable admission boundary. Its terminal
    /// interruption fences checkpoint publication even before service cleanup.
    pub async fn compaction_execution_cancelled(&self, operation: &str) -> Result<bool> {
        repositories::compaction::runner::compaction_execution_cancelled(
            &self.connection,
            operation,
        )
        .await
    }
    pub async fn compaction_bind_execution_turn(&self, operation: &str, turn: &str) -> Result<()> {
        repositories::compaction::runner::compaction_bind_execution_turn(
            &self.connection,
            operation,
            turn,
        )
        .await
    }
    pub async fn compaction_prepare_runner(
        &self,
        operation: &str,
        budget: &ModelBudget,
        source_count: u64,
        reference_count: u64,
    ) -> Result<()> {
        repositories::compaction::runner::compaction_prepare_runner(
            &self.connection,
            operation,
            budget,
            source_count,
            reference_count,
        )
        .await
    }
    pub async fn compaction_runner_plan(
        &self,
        operation: &str,
    ) -> Result<Option<RunnerPlanRecord>> {
        repositories::compaction::runner::compaction_runner_plan(&self.connection, operation).await
    }
    pub async fn compaction_append_manifest(
        &self,
        operation: &str,
        entries: &[ManifestEntry],
    ) -> Result<()> {
        repositories::compaction::runner::compaction_append_manifest(self, operation, entries).await
    }
    pub async fn compaction_activate_runner(
        &self,
        operation: &str,
        initial: &RunnerState,
    ) -> Result<()> {
        repositories::compaction::runner::compaction_activate_runner(self, operation, initial).await
    }
    pub async fn compaction_runner_state(&self, operation: &str) -> Result<Option<RunnerState>> {
        repositories::compaction::runner::compaction_runner_state(&self.connection, operation).await
    }
    /// Complete the state record after a control-plane terminal fence. No
    /// attempt can advance past that fence. Preparation reads one bounded row;
    /// the write revalidates its generation and durable terminal classification.
    pub async fn compaction_reconcile_runner_state(
        &self,
        operation: &str,
    ) -> Result<Option<RunnerState>> {
        repositories::compaction::runner::compaction_reconcile_runner_state(self, operation).await
    }
    pub async fn compaction_manifest_page(
        &self,
        operation: &str,
        reference_only: bool,
        unit: u64,
        source_offset: u32,
    ) -> Result<Vec<ManifestEntry>> {
        repositories::compaction::runner::compaction_manifest_page(
            &self.connection,
            operation,
            reference_only,
            unit,
            source_offset,
        )
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
        repositories::compaction::runner::compaction_runner_transition(
            self, operation, expected, next, candidate,
        )
        .await
    }
    pub(crate) async fn prepare_checkpoint_ancestry(
        &self,
        operation: &str,
        checkpoint: &str,
        generation: u64,
    ) -> Result<()> {
        repositories::compaction::runner::prepare_checkpoint_ancestry(
            &self.connection,
            operation,
            checkpoint,
            generation,
        )
        .await
    }
    /// One atomic domain transition. The immutable, admitted manifest bounds
    /// validation to this operation's selected sources; it never scans transcript
    /// payloads or the complete history. Every source version and the owner head
    /// are checked inside the same transaction which publishes the candidate.
    pub async fn compaction_apply_runner(
        &self,
        operation: &str,
        state: &RunnerState,
        expected_head: Option<&str>,
    ) -> Result<CommitOutcome> {
        repositories::compaction::runner::compaction_apply_runner(
            self,
            operation,
            state,
            expected_head,
        )
        .await
    }
    pub async fn compaction_bound_source_projection(
        &self,
        operation: &str,
    ) -> Result<Option<FrozenHistoryRef>> {
        repositories::compaction::source_projection::compaction_bound_source_projection(
            &self.connection,
            operation,
        )
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
        repositories::compaction::source_projection::compaction_bind_source_projection(
            self, operation, descriptor,
        )
        .await
    }
    /// First accepted manifest wins across retries. This is one metadata write;
    /// source materialization/hashing occurred before acquiring the writer.
    /// All prepared identities are revalidated in the INSERT predicate. This
    /// record alone grants no delivery or access to the source history.
    pub async fn compaction_record_task_output(
        &self,
        workspace: &str,
        task_run_turn: &str,
        history: &FrozenHistoryRef,
    ) -> Result<TaskOutputSnapshot> {
        repositories::compaction::task_output::compaction_record_task_output(
            self,
            workspace,
            task_run_turn,
            history,
        )
        .await
    }
    /// Point metadata read with the complete retained Task/turn relationship.
    /// No result payload or mutable current child transcript is loaded.
    pub async fn compaction_task_output(
        &self,
        workspace: &str,
        task_run_turn: &str,
    ) -> Result<Option<TaskOutputSnapshot>> {
        repositories::compaction::task_output::compaction_task_output(
            &self.connection,
            workspace,
            task_run_turn,
        )
        .await
    }
    pub async fn compaction_delivery_output(
        &self,
        workspace: &str,
        delivery: &str,
    ) -> Result<Option<TaskDeliveryOutputSnapshot>> {
        repositories::compaction::task_output::compaction_delivery_output(self, workspace, delivery)
            .await
    }
    /// Inspect at most 128 event revisions below the common capture fence.
    /// The caller retains the first acknowledgement for each delivery ID across
    /// pages; replayed notifications may occur in later quanta. No payload is read.
    pub async fn compaction_delivered_output_page(
        &self,
        workspace: &str,
        thread: &str,
        after: i64,
        fence: &HistoryReadFence,
    ) -> Result<DeliveredTaskOutputPage> {
        repositories::compaction::task_output::compaction_delivered_output_page(
            &self.connection,
            workspace,
            thread,
            after,
            fence,
        )
        .await
    }
}
