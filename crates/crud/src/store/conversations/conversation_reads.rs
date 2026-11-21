use pioneer_crud_execution::time::unix_to_datetime;
use crate::{
    AgentWorkGraphProjectionTarget, ConversationEntry, CrudStore, MarkThreadReadRequest,
    ProjectionPageAnchor, SavedHistoryKey, SavedHistoryRow, SavedHistorySnapshot,
    ThreadEpisodicCanonicalItem,
    ThreadHistorySnapshot, ThreadTimelineApprovalScope, TurnMessageGuard, TurnWorkOwner,
    count_unread_message_blocks_for_threads, find_thread_timeline_block_by_sort_key,
    find_turn_work_item_projection, find_turn_work_item_projection_by_order_key,
    find_turn_work_projection, list_thread_timeline_blocks_page,
    list_turn_work_item_projections_by_ids, list_turn_work_items_page,
    load_agent_work_graph_projection_for_turn, load_agent_work_graph_projection_target,
};
use anyhow::Result;
use pioneer_crud_access::policy;
use pioneer_crud_conversations::thread_metadata;
use pioneer_crud_conversations::turn_reads;
use pioneer_protocol::{
    PrincipalId, PromptManifest, SandboxMode, Thread, ThreadReadResponse, Turn, TurnItem,
    TurnItemType, TurnItemsResponse, UserInput,
};
use std::collections::HashMap;

impl CrudStore {
    pub async fn mark_thread_read(
        &self,
        request: MarkThreadReadRequest,
    ) -> Result<ThreadReadResponse> {
        pioneer_crud_conversations::thread_read_cursor::mark_thread_read(&self.connection, request)
            .await
    }

    pub async fn unread_counts_for_threads(
        &self,
        principal_id: &PrincipalId,
        thread_ids: &[String],
    ) -> Result<HashMap<String, u64>> {
        count_unread_message_blocks_for_threads(
            &self.connection,
            thread_ids,
            principal_id.to_string().as_str(),
        )
        .await
    }

    pub async fn get_turn(&self, thread_id: &str, turn_id: &str) -> Result<Option<(String, Turn)>> {
        turn_reads::get_turn_for_thread(&self.connection, thread_id, turn_id).await
    }

    pub async fn get_turns_by_thread_and_ids(
        &self,
        thread_id: &str,
        turn_ids: &[String],
    ) -> Result<HashMap<String, Turn>> {
        turn_reads::get_turns_by_thread_and_ids(&self.connection, thread_id, turn_ids).await
    }

    pub async fn get_turn_message_guards_by_thread_and_ids(
        &self,
        thread_id: &str,
        turn_ids: &[String],
    ) -> Result<Vec<TurnMessageGuard>> {
        turn_reads::find_turn_message_guards_by_thread_and_ids(
            &self.connection,
            thread_id,
            turn_ids,
        )
        .await
    }

    pub async fn complete_in_progress_turns_after_final_agent_message(
        &self,
        event_timestamp_secs: i64,
    ) -> Result<u64> {
        pioneer_crud_conversations::conversation_repair::complete_in_progress_turns_after_final_agent_message(
            &self.connection,
            event_timestamp_secs,
            |notification, at| self.materialize_turn_completed(notification, at),
        )
        .await
    }

    pub async fn reconcile_thread_foreground_statuses(
        &self,
        event_timestamp_secs: i64,
    ) -> Result<u64> {
        pioneer_crud_conversations::conversation_repair::reconcile_thread_foreground_statuses(
            &self.connection,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn get_turn_inputs(&self, turn_id: &str) -> Result<Vec<UserInput>> {
        turn_reads::get_turn_inputs(&self.connection, turn_id).await
    }

    pub async fn post_turn_tool_summaries(
        &self,
        turn_id: &str,
        limit: usize,
    ) -> Result<Vec<pioneer_crud_conversations::tool_summary::PostTurnToolSummary>> {
        pioneer_crud_conversations::tool_summary::list(&self.connection, turn_id, limit).await
    }

    pub async fn post_turn_user_text(
        &self,
        turn_id: &str,
        max_chars: usize,
    ) -> Result<(String, bool)> {
        pioneer_crud_conversations::turn_input::post_turn_user_text(
            &self.connection,
            turn_id,
            max_chars,
        )
        .await
    }

    pub async fn get_turn_inputs_for_turns(
        &self,
        turn_ids: &[String],
    ) -> Result<HashMap<String, Vec<UserInput>>> {
        turn_reads::get_turn_inputs_for_turns(&self.connection, turn_ids).await
    }

    pub async fn update_turn_prompt_manifest(
        &self,
        thread_id: &str,
        turn_id: &str,
        manifest: &PromptManifest,
        event_timestamp_secs: i64,
    ) -> Result<bool> {
        pioneer_crud_conversations::turn_writes::write_turn_prompt_manifest(
            &self.connection,
            thread_id,
            turn_id,
            manifest,
            event_timestamp_secs,
        )
        .await
    }

    pub async fn get_turn_location(&self, turn_id: &str) -> Result<Option<(String, String)>> {
        turn_reads::get_turn_location(&self.connection, turn_id).await
    }

    pub async fn get_turn_item_type(
        &self,
        turn_id: &str,
        item_id: &str,
    ) -> Result<Option<TurnItemType>> {
        turn_reads::get_turn_item_type(&self.connection, turn_id, item_id).await
    }

    pub async fn get_turn_item(
        &self,
        turn_id: &str,
        item_id: &str,
    ) -> Result<Option<pioneer_protocol::TurnItem>> {
        turn_reads::get_turn_item(&self.connection, turn_id, item_id).await
    }

    pub async fn get_thread_episodic_canonical_item(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
    ) -> Result<Option<ThreadEpisodicCanonicalItem>> {
        self.memory_store()
            .get_thread_episodic_canonical_item(workspace_id, thread_id, turn_id, item_id)
            .await
    }

    /// Requested page items only. Bodies/status and last-final metadata share
    /// each bounded query snapshot; no reader is held during JSON decoding.
    pub(crate) fn conversation_display_store(
        &self,
    ) -> pioneer_crud_timeline::conversation_display::ConversationDisplayStore<'_> {
        pioneer_crud_timeline::conversation_display::ConversationDisplayStore::new(&self.connection)
    }

    pub async fn get_timeline_assistant_items(
        &self,
        workspace_id: &str,
        thread_id: &str,
        targets: &[(String, String)],
    ) -> Result<HashMap<(String, String), (TurnItem, Option<String>, bool)>> {
        self.conversation_display_store()
            .get_timeline_assistant_items(workspace_id, thread_id, targets)
            .await
    }

    pub async fn get_turn_items_by_ids(
        &self,
        turn_id: &str,
        item_ids: &[String],
    ) -> Result<HashMap<String, pioneer_protocol::TurnItem>> {
        self.conversation_display_store()
            .get_turn_items_by_ids(turn_id, item_ids)
            .await
    }

    pub async fn list_turn_items_by_type(
        &self,
        turn_id: &str,
        item_type: &str,
    ) -> Result<Vec<TurnItem>> {
        turn_reads::list_decoded_turn_items_by_type(&self.connection, turn_id, item_type).await
    }

    pub async fn list_turn_items_by_type_for_turns(
        &self,
        turn_ids: &[String],
        item_type: &str,
    ) -> Result<HashMap<String, Vec<TurnItem>>> {
        self.conversation_display_store()
            .list_turn_items_by_type_for_turns(turn_ids, item_type)
            .await
    }

    /// Returns payload and lifecycle status from the same bounded source read.
    pub async fn list_turn_items_with_status_by_type_for_turns(
        &self,
        turn_ids: &[String],
        item_type: &str,
    ) -> Result<HashMap<String, Vec<(TurnItem, Option<String>)>>> {
        self.conversation_display_store()
            .list_turn_items_with_status_by_type_for_turns(turn_ids, item_type)
            .await
    }

    pub async fn list_completed_agent_messages(&self, turn_id: &str) -> Result<Vec<TurnItem>> {
        self.conversation_display_store()
            .list_completed_agent_messages(turn_id)
            .await
    }

    /// The answer accepted by the completed Turn's finalization commit. Legacy
    /// workers have no intent: only an unambiguous FinalAnswer is usable there.
    /// Read through this store's scheduling scope; decode after fetching rows.
    pub async fn completed_turn_final_answer(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<TurnItem>> {
        self.conversation_display_store()
            .completed_turn_final_answer(workspace_id, thread_id, turn_id)
            .await
    }

    pub async fn get_turn_item_events(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<TurnItemsResponse>> {
        self.get_turn_item_events_page(thread_id, turn_id, None, 200)
            .await
    }

    pub async fn get_turn_item_events_page(
        &self,
        thread_id: &str,
        turn_id: &str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> Result<Option<TurnItemsResponse>> {
        pioneer_crud_conversations::turn_item_events::get_turn_item_events_page(
            &self.connection,
            thread_id,
            turn_id,
            after_sequence,
            limit,
        )
        .await
    }

    pub async fn get_thread_conversation_history(
        &self,
        thread_id: &str,
        max_turns: usize,
    ) -> Result<Vec<ConversationEntry>> {
        self.conversation_display_store()
            .get_thread_conversation_history(thread_id, max_turns)
            .await
    }

    pub async fn get_thread_conversation_history_with_artifacts(
        &self,
        workspace_id: &str,
        thread_id: &str,
        max_turns: usize,
    ) -> Result<Vec<ConversationEntry>> {
        self.conversation_display_store()
            .get_thread_conversation_history_with_artifacts(workspace_id, thread_id, max_turns)
            .await
    }

    pub async fn get_thread_causally_closed_conversation_history(
        &self,
        thread_id: &str,
        max_entries: usize,
    ) -> Result<Vec<ConversationEntry>> {
        self.conversation_display_store()
            .get_thread_causally_closed_conversation_history(thread_id, max_entries)
            .await
    }

    pub async fn get_thread_causally_closed_conversation_history_with_artifacts(
        &self,
        workspace_id: &str,
        thread_id: &str,
        max_entries: usize,
    ) -> Result<Vec<ConversationEntry>> {
        self.conversation_display_store()
            .get_thread_causally_closed_conversation_history_with_artifacts(
                workspace_id,
                thread_id,
                max_entries,
            )
            .await
    }

    pub async fn get_first_thread_user_text(&self, thread_id: &str) -> Result<Option<String>> {
        self.conversation_display_store()
            .get_first_thread_user_text(thread_id)
            .await
    }

    pub async fn get_thread_by_id(
        &self,
        thread_id: &str,
    ) -> Result<Option<pioneer_entity::thread::Model>> {
        thread_metadata::find_thread_by_id(&self.connection, thread_id).await
    }

    pub async fn list_thread_timeline_projection_page(
        &self,
        thread_id: &str,
        approval_scope: Option<&ThreadTimelineApprovalScope>,
        anchor: ProjectionPageAnchor<'_>,
        limit: u64,
    ) -> Result<Vec<pioneer_entity::thread_timeline_block::Model>> {
        list_thread_timeline_blocks_page(&self.connection, thread_id, approval_scope, anchor, limit)
            .await
    }

    pub async fn find_thread_timeline_projection_block_by_sort_key(
        &self,
        thread_id: &str,
        approval_scope: Option<&ThreadTimelineApprovalScope>,
        sort_key: &str,
    ) -> Result<Option<pioneer_entity::thread_timeline_block::Model>> {
        find_thread_timeline_block_by_sort_key(
            &self.connection,
            thread_id,
            approval_scope,
            sort_key,
        )
        .await
    }

    pub async fn get_turn_work_projection(
        &self,
        turn_id: &str,
    ) -> Result<Option<pioneer_entity::turn_work_projection::Model>> {
        find_turn_work_projection(&self.connection, turn_id).await
    }

    pub async fn get_turn_work_owner(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<TurnWorkOwner>> {
        turn_reads::find_turn_work_owner(&self.connection, workspace_id, thread_id, turn_id).await
    }

    pub async fn get_detached_turn_work_tombstone(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<(i64, i64)>> {
        pioneer_crud_conversations::projection_stream::find_detached_work_tombstone(
            &self.connection,
            workspace_id,
            thread_id,
            turn_id,
        )
        .await
    }

    pub async fn get_agent_work_graph_projection_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Option<pioneer_protocol::AgentWorkGraphProjection>> {
        load_agent_work_graph_projection_for_turn(&self.connection, turn_id).await
    }

    pub async fn get_agent_work_graph_projection_target(
        &self,
        root_execution_id: &str,
    ) -> Result<Option<AgentWorkGraphProjectionTarget>> {
        load_agent_work_graph_projection_target(&self.connection, root_execution_id).await
    }

    pub async fn get_turn_work_item_projection(
        &self,
        work_item_id: &str,
    ) -> Result<Option<pioneer_entity::turn_work_item_projection::Model>> {
        find_turn_work_item_projection(&self.connection, work_item_id).await
    }

    pub async fn list_turn_work_item_projections_by_ids(
        &self,
        turn_id: &str,
        work_item_ids: &[String],
        visibility: Option<&str>,
    ) -> Result<Vec<pioneer_entity::turn_work_item_projection::Model>> {
        list_turn_work_item_projections_by_ids(&self.connection, turn_id, work_item_ids, visibility)
            .await
    }

    pub async fn find_turn_work_item_projection_by_order_key(
        &self,
        turn_id: &str,
        order_key: &str,
        visibility: Option<&str>,
    ) -> Result<Option<pioneer_entity::turn_work_item_projection::Model>> {
        find_turn_work_item_projection_by_order_key(
            &self.connection,
            turn_id,
            order_key,
            visibility,
        )
        .await
    }

    pub async fn list_turn_work_item_projection_page(
        &self,
        turn_id: &str,
        visibility: Option<&str>,
        anchor: ProjectionPageAnchor<'_>,
        limit: u64,
    ) -> Result<Vec<pioneer_entity::turn_work_item_projection::Model>> {
        list_turn_work_items_page(&self.connection, turn_id, visibility, anchor, limit).await
    }

    pub async fn get_thread_sandbox_mode(&self, thread_id: &str) -> Result<Option<SandboxMode>> {
        policy::find_thread_sandbox_mode(&self.connection, thread_id).await
    }

    pub async fn latest_turn_id_by_creation_order(
        &self,
        thread_id: &str,
    ) -> Result<Option<String>> {
        pioneer_crud_conversations::turn_order::latest_turn_id_by_creation_order(
            &self.connection, thread_id,
        )
        .await
    }

    pub async fn turn_before_launch_and_intervening_by_creation_order(
        &self,
        thread_id: &str,
        launch_turn_id: &str,
        allowed_task_run_turn_id: &str,
    ) -> Result<Option<(Option<String>, bool)>> {
        pioneer_crud_conversations::turn_order::turn_before_launch_and_intervening_by_creation_order(
            &self.connection, thread_id, launch_turn_id, allowed_task_run_turn_id,
        )
        .await
    }

    pub async fn turn_ids_after_by_creation_order(
        &self,
        thread_id: &str,
        source_turn_id: &str,
    ) -> Result<Vec<String>> {
        pioneer_crud_conversations::turn_order::turn_ids_after_by_creation_order(
            &self.connection, thread_id, source_turn_id,
        )
        .await
    }

    pub async fn get_thread_model(&self, thread_id: &str) -> Result<Option<Thread>> {
        pioneer_crud_timeline::thread_display::ThreadDisplayStore::new(&self.connection)
            .get_thread_model(thread_id)
            .await
    }

    pub async fn get_thread_summary(&self, thread_id: &str) -> Result<Option<(String, i64)>> {
        thread_metadata::get_thread_summary(&self.connection, thread_id).await
    }

    pub async fn update_thread_summary(
        &self,
        thread_id: &str,
        summary: &str,
        turn_count: i64,
    ) -> Result<()> {
        self.run_serialized_write(|| async {
            let now = unix_to_datetime(chrono::Utc::now().timestamp());
            thread_metadata::update_thread_summary(
                &self.connection,
                thread_id,
                summary,
                turn_count,
                now,
            )
            .await
        })
        .await
    }

    pub async fn count_completed_turns(&self, thread_id: &str) -> Result<u64> {
        turn_reads::count_completed_turns_for_thread(&self.connection, thread_id).await
    }

    pub async fn count_recovery_jobs_for_turn(&self, turn_id: &str) -> Result<u64> {
        self.recovery_store()
            .count_recovery_jobs_for_turn(turn_id)
            .await
    }

    pub async fn get_turns_for_summary(
        &self,
        thread_id: &str,
        skip: u64,
        take: u64,
    ) -> Result<Vec<ConversationEntry>> {
        self.conversation_display_store()
            .get_turns_for_summary(thread_id, skip, take)
            .await
    }

    pub async fn get_thread_history(
        &self,
        thread_id: &str,
        limit_events: Option<u64>,
    ) -> Result<Option<ThreadHistorySnapshot>> {
        pioneer_crud_conversations::thread_history::get_thread_history(
            &self.connection,
            thread_id,
            limit_events,
        )
        .await
    }

    /// Capture the history frontier and revision under one read-only snapshot.
    pub async fn saved_history_start(
        &self,
        thread_id: &str,
    ) -> Result<Option<(SavedHistorySnapshot, SavedHistoryKey, String)>> {
        pioneer_crud_conversations::saved_history::saved_history_start(&self.connection, thread_id)
            .await
    }

    pub async fn saved_history_snapshot(&self, thread_id: &str) -> Result<SavedHistorySnapshot> {
        pioneer_crud_conversations::saved_history::snapshot(&self.connection, thread_id).await
    }

    pub async fn saved_history_page_at_snapshot(
        &self,
        thread_id: &str,
        boundary: Option<&SavedHistoryKey>,
        before: Option<&SavedHistoryKey>,
        limit: u64,
        snapshot: &SavedHistorySnapshot,
    ) -> Result<Vec<SavedHistoryRow>> {
        pioneer_crud_conversations::saved_history::page(
            &self.connection,
            thread_id,
            boundary,
            before,
            limit,
            snapshot,
        )
        .await
    }

    /// Directional metadata-only keyset page within the frozen saved set.
    pub async fn saved_history_page_direction_at_snapshot(
        &self,
        thread_id: &str,
        boundary: Option<&SavedHistoryKey>,
        position: Option<&SavedHistoryKey>,
        inclusive: bool,
        newer: bool,
        limit: u64,
        snapshot: &SavedHistorySnapshot,
    ) -> Result<Vec<SavedHistoryRow>> {
        pioneer_crud_conversations::saved_history::page_direction(
            &self.connection,
            thread_id,
            boundary,
            position,
            inclusive,
            newer,
            limit,
            snapshot,
        )
        .await
    }

    pub async fn saved_history_anchor(
        &self,
        thread_id: &str,
        turn_id: &str,
        item_id: Option<&str>,
        snapshot: &SavedHistorySnapshot,
    ) -> Result<Option<SavedHistoryRow>> {
        pioneer_crud_conversations::saved_history::anchor(
            &self.connection,
            thread_id,
            turn_id,
            item_id,
            snapshot,
        )
        .await
    }

    pub async fn saved_history_revision_at_snapshot(
        &self,
        thread_id: &str,
        boundary: &SavedHistoryKey,
        snapshot: &SavedHistorySnapshot,
    ) -> Result<String> {
        pioneer_crud_conversations::saved_history::revision(
            &self.connection,
            thread_id,
            boundary,
            snapshot,
        )
        .await
    }

    pub async fn saved_history_input(
        &self,
        turn_id: &str,
        input_id: &str,
    ) -> Result<Option<UserInput>> {
        pioneer_crud_conversations::saved_history::saved_history_input(
            &self.connection,
            turn_id,
            input_id,
        )
        .await
    }
}
