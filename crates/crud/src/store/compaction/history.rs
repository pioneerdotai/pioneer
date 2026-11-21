use crate::CrudStore;
use anyhow::Result;
use pioneer_compaction::SourceRef;
use pioneer_crud_compaction::canonical_history::{CanonicalFragment, PagedSource, SourcePage};
use pioneer_crud_compaction::history::{
    HistoryCausalBoundary, HistoryReadFence, HistoryTurnBoundary,
};
use pioneer_crud_compaction::source_revision::HistoricalEventProjection;
use pioneer_crud_compaction::task_output::{DeliveredTaskOutputCursor, DeliveredTaskOutputPage};

impl CrudStore {
    pub async fn compaction_assert_history_scope(
        &self,
        workspace: &str,
        thread: &str,
    ) -> Result<()> {
        self.compaction_store()
            .compaction_assert_history_scope(workspace, thread)
            .await
    }

    /// Canonical ownership and disclosure metadata, independent of full result
    /// payload size. Every canonical tool item is visible in timeline projection;
    /// non-tool/hidden system records are excluded in the repository query.
    pub async fn tool_result_access_source(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<serde_json::Value>> {
        pioneer_crud_conversations::tool_result_access::tool_result_access_source(
            &self.connection,
            workspace,
            thread,
            turn,
            item,
        )
        .await
    }

    /// Pin append-only discovery to a bounded high water mark. Edits remain
    /// governed by each source revision, not by this sequence boundary.
    pub async fn compaction_source_high_water(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
    ) -> Result<i64> {
        self.compaction_store()
            .compaction_source_high_water(workspace, thread, turn, kind)
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
        self.compaction_store()
            .compaction_source_page(workspace, thread, turn, kind, after)
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
        self.compaction_store()
            .compaction_source_metadata_page(workspace, thread, turn, kind, after)
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
        self.compaction_store()
            .compaction_source_metadata_page_at_fence(
                workspace,
                thread,
                turn,
                kind,
                after,
                capture_order,
            )
            .await
    }

    /// Apply the historical cutoff during metadata discovery, before payloads.
    pub async fn compaction_source_metadata_page_before(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        kind: PagedSource,
        after: i64,
        capture_order: i64,
        before: Option<sea_orm::prelude::DateTimeWithTimeZone>,
    ) -> Result<SourcePage> {
        self.compaction_store()
            .compaction_source_metadata_page_before(
                workspace,
                thread,
                turn,
                kind,
                after,
                capture_order,
                before,
            )
            .await
    }

    pub async fn compaction_item_created_at(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        id: &str,
    ) -> Result<Option<sea_orm::prelude::DateTimeWithTimeZone>> {
        self.compaction_store()
            .compaction_item_created_at(workspace, thread, turn, id)
            .await
    }

    /// Validate directly consumed identities. Raw sources remain exact-current;
    /// checkpoints are validated as independently published objects.
    pub async fn compaction_sources_current(
        &self,
        workspace: &str,
        thread: &str,
        sources: &[SourceRef],
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_sources_current(workspace, thread, sources)
            .await
    }

    pub async fn compaction_model_context_sources_current(
        &self,
        workspace: &str,
        thread: &str,
        sources: &[SourceRef],
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_model_context_sources_current(workspace, thread, sources)
            .await
    }

    pub async fn compaction_historical_event_projections(
        &self,
        workspace: &str,
        thread: &str,
        sources: &[SourceRef],
    ) -> Result<Vec<HistoricalEventProjection>> {
        self.compaction_store()
            .compaction_historical_event_projections(workspace, thread, sources)
            .await
    }

    /// Resolve a directly consumed source's current owner. Historical checkpoint
    /// coverage uses the source thread saved in its publication manifest.
    pub async fn compaction_reference_thread(
        &self,
        workspace: &str,
        source: &SourceRef,
    ) -> Result<Option<String>> {
        self.compaction_store()
            .compaction_reference_thread(workspace, source)
            .await
    }

    pub async fn compaction_head(&self, owner: &str) -> Result<Option<String>> {
        self.compaction_store().compaction_head(owner).await
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
        self.compaction_store()
            .compaction_source_fragment(
                workspace,
                thread,
                turn,
                id,
                expected_revision,
                character_offset,
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
        self.compaction_store()
            .compaction_tool_result_id(workspace, thread, turn, item)
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
        self.compaction_store()
            .compaction_tool_result_fragment(
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
        self.compaction_store()
            .compaction_reference_fragment(workspace, thread, reference, character_offset)
            .await
    }

    pub async fn compaction_reference_payload(
        &self,
        workspace: &str,
        thread: &str,
        reference: &SourceRef,
    ) -> Result<Option<String>> {
        self.compaction_store()
            .compaction_reference_payload(workspace, thread, reference)
            .await
    }

    pub async fn compaction_reference_payload_batch(
        &self,
        workspace: &str,
        thread: &str,
        references: &[SourceRef],
    ) -> Result<(usize, Vec<String>)> {
        self.compaction_store()
            .compaction_reference_payload_batch(workspace, thread, references)
            .await
    }

    pub async fn compaction_references_current(
        &self,
        workspace: &str,
        thread: &str,
        references: &[SourceRef],
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_references_current(workspace, thread, references)
            .await
    }

    pub async fn compaction_projection_version(
        &self,
        workspace: &str,
        thread: &str,
    ) -> Result<u64> {
        self.compaction_store()
            .compaction_projection_version(workspace, thread)
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
        self.compaction_store()
            .compaction_context_reference_for_item(workspace, thread, turn, item, source)
            .await
    }

    pub async fn compaction_tool_item_reference(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<SourceRef>> {
        self.compaction_store()
            .compaction_tool_item_reference(workspace, thread, turn, item)
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
        self.compaction_store()
            .compaction_replay_item_id(workspace, thread, source)
            .await
    }

    pub async fn compaction_item_reference(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        item: &str,
    ) -> Result<Option<SourceRef>> {
        self.compaction_store()
            .compaction_item_reference(workspace, thread, turn, item)
            .await
    }

    pub async fn compaction_turn_is_completed(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_turn_is_completed(workspace, thread, turn)
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
        self.compaction_store()
            .compaction_record_event_projection(workspace, thread, source, event)
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
        self.compaction_store()
            .compaction_history_causal_boundary(workspace, thread, turn, fence)
            .await
    }

    /// Resolve the durable parent and initial child turn without reading Task input.
    pub async fn compaction_child_history_parent(
        &self,
        workspace: &str,
        child: &str,
    ) -> Result<Option<pioneer_crud_compaction::history::ChildHistoryParent>> {
        self.compaction_store()
            .compaction_child_history_parent(workspace, child)
            .await
    }

    pub async fn compaction_history_source_created_at(
        &self,
        workspace: &str,
        thread: &str,
        source: &SourceRef,
    ) -> Result<Option<sea_orm::prelude::DateTimeWithTimeZone>> {
        self.compaction_store()
            .compaction_history_source_created_at(workspace, thread, source)
            .await
    }

    /// One read fixes the append boundary before enumerating any turns. Source
    /// bounds use retained metadata with explicit insertion order; deleting or
    /// vacuuming canonical rows cannot change that order. MAX uses indexes and
    /// reads no payload. Every subsequent discovery checks its workspace scope.
    pub async fn compaction_history_read_fence(&self) -> Result<HistoryReadFence> {
        self.compaction_store()
            .compaction_history_read_fence()
            .await
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
        self.compaction_store()
            .compaction_history_turn_page(workspace, thread, after, fence)
            .await
    }

    /// Metadata only for exact accepted turns; unrelated parent turns never
    /// enter the boundary expressions or cross the reader pool.
    pub async fn compaction_history_selected_turn_page(
        &self,
        workspace: &str,
        thread: &str,
        selected: &[String],
        fence: &HistoryReadFence,
    ) -> Result<Vec<HistoryTurnBoundary>> {
        self.compaction_store()
            .compaction_history_selected_turn_page(workspace, thread, selected, fence)
            .await
    }

    /// Select at most one local delivery Turn and 127 event metadata rows.
    /// Callers order acknowledgements by capture_order across all local pages
    /// before selecting the first per delivery. No payload is read.
    pub async fn compaction_delivered_output_page(
        &self,
        workspace: &str,
        thread: &str,
        cursor: &DeliveredTaskOutputCursor,
        fence: &HistoryReadFence,
    ) -> Result<DeliveredTaskOutputPage> {
        self.compaction_store()
            .compaction_delivered_output_page(workspace, thread, cursor, fence)
            .await
    }

    /// Revalidate only the metadata sources selected by the original page.
    /// Append never changes the selected set or its saved continuation.
    pub async fn compaction_recheck_delivered_output_page(
        &self,
        workspace: &str,
        thread: &str,
        page: &DeliveredTaskOutputPage,
        fence: &HistoryReadFence,
    ) -> Result<DeliveredTaskOutputPage> {
        self.compaction_store()
            .compaction_recheck_delivered_output_page(workspace, thread, page, fence)
            .await
    }

    pub async fn compaction_begin_history_attempt(
        &self,
        id: &str,
        revision: i64,
        descriptor: &str,
        hash: &str,
        now: i64,
    ) -> Result<Option<i64>> {
        self.compaction_store()
            .compaction_begin_history_attempt(id, revision, descriptor, hash, now)
            .await
    }

    pub async fn compaction_record_history_result(
        &self,
        id: &str,
        revision: i64,
        failures: i64,
        outcome: pioneer_crud_compaction::history_check_result::HistoryCheckOutcome,
        diagnostic: &pioneer_crud_compaction::history_check_result::HistoryCheckDiagnostic,
        now: i64,
    ) -> Result<bool> {
        self.compaction_store()
            .compaction_record_history_result(id, revision, failures, outcome, diagnostic, now)
            .await
    }
}
