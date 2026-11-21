use crate::{
    CliRuntimeActiveTurnBindingStatus, CliRuntimeExecutionSegmentRecord,
    CliRuntimeExecutionSegmentStatus, CliRuntimeInstructionProjectionRecord,
    CliRuntimeNativeTurnOwner, CliRuntimeThreadBindingRecord, CliRuntimeThreadMcpMetadata,
    CliRuntimeTurnAttemptRecord, CliRuntimeTurnAttemptStatus, CliRuntimeTurnBindingListFilter,
    CliRuntimeTurnBindingRecord, CliRuntimeTurnMcpMetadata, CliRuntimeTurnTerminalGuard, CrudStore,
    NewCliRuntimeInstructionProjection, NewCliRuntimeThreadBinding, NewCliRuntimeTurnBinding,
    PrepareClaudeProviderSessionBinding, PreparedClaudeProviderSessionBinding,
};
use anyhow::{Context, Result, bail};
use pioneer_crud_execution::cli_runtime_binding;
use pioneer_crud_packages::turn_mcp_binding;
use sea_orm::TransactionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

impl CrudStore {
    pub(crate) fn cli_runtime_store(
        &self,
    ) -> pioneer_crud_execution::cli_runtime_store::CliRuntimeStore {
        pioneer_crud_execution::cli_runtime_store::CliRuntimeStore::new(self.connection.clone())
    }

    pub async fn insert_cli_runtime_instruction_projection_if_absent(
        &self,
        projection: NewCliRuntimeInstructionProjection,
    ) -> Result<CliRuntimeInstructionProjectionRecord> {
        self.cli_runtime_store()
            .insert_cli_runtime_instruction_projection_if_absent(projection)
            .await
    }

    pub async fn get_cli_runtime_instruction_projection(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeInstructionProjectionRecord>> {
        self.cli_runtime_store()
            .get_cli_runtime_instruction_projection(turn_id)
            .await
    }

    pub async fn upsert_cli_runtime_thread_binding(
        &self,
        binding: NewCliRuntimeThreadBinding,
    ) -> Result<CliRuntimeThreadBindingRecord> {
        self.compaction_store()
            .upsert_cli_runtime_thread_binding(binding)
            .await
    }

    pub async fn insert_cli_fork_intent_if_absent(
        &self,
        pending: NewCliRuntimeThreadBinding,
    ) -> Result<bool> {
        self.compaction_store()
            .insert_cli_fork_intent_if_absent(pending)
            .await
    }

    pub async fn get_cli_runtime_thread_binding(
        &self,
        thread_id: &str,
    ) -> Result<Option<CliRuntimeThreadBindingRecord>> {
        self.compaction_store()
            .get_cli_runtime_thread_binding(thread_id)
            .await
    }

    pub async fn discard_rejected_cli_fork_intent(
        &self,
        thread_id: &str,
        runtime_id: &str,
        source_native_thread_id: &str,
        expected_cursor_json: &str,
    ) -> Result<bool> {
        self.compaction_store()
            .discard_rejected_cli_fork_intent(
                thread_id,
                runtime_id,
                source_native_thread_id,
                expected_cursor_json,
            )
            .await
    }

    pub async fn confirm_marked_cli_fork_intent(
        &self,
        pending: &CliRuntimeThreadBindingRecord,
        fork_native_thread_id: &str,
        native_cwd: Option<String>,
        native_model: Option<String>,
        resume_cursor_json: String,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<CliRuntimeThreadBindingRecord> {
        self.compaction_store()
            .confirm_marked_cli_fork_intent(
                pending,
                fork_native_thread_id,
                native_cwd,
                native_model,
                resume_cursor_json,
                updated_at,
            )
            .await
    }

    pub async fn update_cli_runtime_thread_resume_cursor(
        &self,
        thread_id: &str,
        expected_native_thread_id: &str,
        expected_cursor_json: &str,
        resume_cursor_json: String,
        updated_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<CliRuntimeThreadBindingRecord> {
        self.compaction_store()
            .update_cli_runtime_thread_resume_cursor(
                thread_id,
                expected_native_thread_id,
                expected_cursor_json,
                resume_cursor_json,
                updated_at,
            )
            .await
    }

    pub async fn prepare_claude_provider_session_binding(
        &self,
        request: PrepareClaudeProviderSessionBinding,
    ) -> Result<PreparedClaudeProviderSessionBinding> {
        self.compaction_store()
            .prepare_claude_provider_session_binding(request)
            .await
    }

    pub async fn verify_claude_provider_session_binding(
        &self,
        thread_id: &str,
        expected_provider_session_id: &str,
        emitted_provider_session_id: Option<&str>,
        process_generation: i64,
    ) -> Result<CliRuntimeThreadBindingRecord> {
        self.cli_runtime_store()
            .verify_claude_provider_session_binding(
                thread_id,
                expected_provider_session_id,
                emitted_provider_session_id,
                process_generation,
            )
            .await
    }

    pub async fn set_cli_runtime_thread_mcp_metadata(
        &self,
        thread_id: &str,
        metadata: Option<CliRuntimeThreadMcpMetadata>,
    ) -> Result<CliRuntimeThreadBindingRecord> {
        self.cli_runtime_store()
            .set_cli_runtime_thread_mcp_metadata(thread_id, metadata)
            .await
    }

    pub async fn get_cli_runtime_thread_binding_by_native_thread(
        &self,
        runtime_id: &str,
        native_thread_id: &str,
    ) -> Result<Option<CliRuntimeThreadBindingRecord>> {
        self.cli_runtime_store()
            .get_cli_runtime_thread_binding_by_native_thread(runtime_id, native_thread_id)
            .await
    }

    pub async fn list_cli_runtime_thread_bindings_for_runtime(
        &self,
        workspace_id: &str,
        runtime_id: &str,
    ) -> Result<Vec<CliRuntimeThreadBindingRecord>> {
        self.cli_runtime_store()
            .list_cli_runtime_thread_bindings_for_runtime(workspace_id, runtime_id)
            .await
    }

    pub async fn upsert_cli_runtime_turn_binding(
        &self,
        binding: NewCliRuntimeTurnBinding,
    ) -> Result<CliRuntimeTurnBindingRecord> {
        self.compaction_store()
            .upsert_cli_runtime_turn_binding(binding)
            .await
    }

    pub async fn get_cli_runtime_turn_binding(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .get_cli_runtime_turn_binding(turn_id)
            .await
    }

    pub async fn has_cli_runtime_dispatch_at_or_after(
        &self,
        source: &CliRuntimeTurnBindingRecord,
    ) -> Result<bool> {
        self.cli_runtime_store()
            .has_cli_runtime_dispatch_at_or_after(source)
            .await
    }

    pub async fn set_cli_runtime_turn_mcp_metadata(
        &self,
        turn_id: &str,
        metadata: Option<CliRuntimeTurnMcpMetadata>,
    ) -> Result<CliRuntimeTurnBindingRecord> {
        self.cli_runtime_store()
            .set_cli_runtime_turn_mcp_metadata(turn_id, metadata)
            .await
    }

    pub async fn set_cli_runtime_turn_native_goal_state(
        &self,
        turn_id: &str,
        status: Option<String>,
        native_goal_turn_id: Option<String>,
        observed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        expected: Option<CliRuntimeTurnTerminalGuard>,
        execution_owner_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .set_cli_runtime_turn_native_goal_state(
                turn_id,
                status,
                native_goal_turn_id,
                observed_at,
                expected,
                execution_owner_id,
            )
            .await
    }

    /// Control transition: the returned binding omits mapping bytes and read holds.
    pub async fn set_cli_runtime_turn_native_goal_control_state(
        &self,
        turn_id: &str,
        status: Option<String>,
        native_goal_turn_id: Option<String>,
        observed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
        expected: Option<CliRuntimeTurnTerminalGuard>,
        execution_owner_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .set_cli_runtime_turn_native_goal_control_state(
                turn_id,
                status,
                native_goal_turn_id,
                observed_at,
                expected,
                execution_owner_id,
            )
            .await
    }

    /// Atomically binds the runtime turn metadata and the same activation
    /// generation to every tool in the already-frozen MCP projection.
    pub async fn bind_cli_runtime_turn_mcp_activation(
        &self,
        turn_id: &str,
        metadata: CliRuntimeTurnMcpMetadata,
    ) -> Result<CliRuntimeTurnBindingRecord> {
        let turn_id = turn_id.to_owned();
        let result = self.run_serialized_write(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin CLI MCP activation binding transaction")?;
            let projection = turn_mcp_binding::find_turn_mcp_projection(
                &transaction,
                turn_id.as_str(),
            )
            .await?
            .context("frozen MCP projection is missing for CLI activation")?;
            if projection.manifest_hash != metadata.manifest_hash {
                bail!("CLI MCP activation manifest does not match the frozen projection");
            }
            let updated = turn_mcp_binding::set_projection_activation_generation(
                &transaction,
                turn_id.as_str(),
                metadata.projection_activation_generation,
            )
            .await?;
            let expected = u64::try_from(projection.tool_count)
                .context("frozen MCP projection has an invalid tool count")?;
            if updated != expected {
                bail!(
                    "CLI MCP activation updated {updated} bindings but frozen projection declares {expected}"
                );
            }
            let binding = cli_runtime_binding::set_turn_mcp_metadata(
                &transaction,
                turn_id.as_str(),
                Some(metadata.clone()),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit CLI MCP activation binding transaction")?;
            Ok(binding)
        })
        .await?;
        cli_runtime_binding::hydrate_record(&self.connection, result).await
    }

    pub async fn get_cli_runtime_turn_binding_by_request(
        &self,
        request_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .get_cli_runtime_turn_binding_by_request(request_id)
            .await
    }

    pub async fn get_cli_runtime_turn_binding_by_native_turn(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .get_cli_runtime_turn_binding_by_native_turn(runtime_id, native_turn_id)
            .await
    }

    pub async fn list_cli_runtime_turn_bindings_for_thread(
        &self,
        thread_id: &str,
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .list_cli_runtime_turn_bindings_for_thread(thread_id)
            .await
    }

    /// Bounded global background discovery. Foreground listing retains its API.
    pub async fn list_active_cli_runtime_turn_binding_page(
        &self,
        status: CliRuntimeActiveTurnBindingStatus,
        after: Option<&(sea_orm::entity::prelude::DateTimeWithTimeZone, String)>,
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .list_active_cli_runtime_turn_binding_page(status, after)
            .await
    }

    pub async fn active_cli_runtime_turn_binding_frontier(
        &self,
        status: CliRuntimeActiveTurnBindingStatus,
    ) -> Result<Option<(sea_orm::entity::prelude::DateTimeWithTimeZone, String)>> {
        self.cli_runtime_store()
            .active_cli_runtime_turn_binding_frontier(status)
            .await
    }

    pub async fn list_active_cli_runtime_turn_binding_page_through(
        &self,
        status: CliRuntimeActiveTurnBindingStatus,
        after: Option<&(sea_orm::entity::prelude::DateTimeWithTimeZone, String)>,
        through: &(sea_orm::entity::prelude::DateTimeWithTimeZone, String),
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .list_active_cli_runtime_turn_binding_page_through(status, after, through)
            .await
    }

    /// Background control projection. `input_mapping_json` is absent, including
    /// for inline storage; payload holds/decompression are not part of discovery.
    pub async fn list_active_cli_runtime_turn_binding_control_page_through(
        &self,
        status: CliRuntimeActiveTurnBindingStatus,
        after: Option<&(DateTimeWithTimeZone, String)>,
        through: &(DateTimeWithTimeZone, String),
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .list_active_cli_runtime_turn_binding_control_page_through(status, after, through)
            .await
    }

    /// Metadata-only counterpart of the ordinary logical binding read.
    pub async fn get_cli_runtime_turn_binding_control(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .get_cli_runtime_turn_binding_control(turn_id)
            .await
    }

    pub async fn list_cli_runtime_turn_bindings(
        &self,
        filter: CliRuntimeTurnBindingListFilter,
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .list_cli_runtime_turn_bindings(filter)
            .await
    }

    /// Routing/status discovery without mapping bytes or payload read holds.
    pub async fn list_cli_runtime_turn_binding_controls(
        &self,
        filter: CliRuntimeTurnBindingListFilter,
    ) -> Result<Vec<CliRuntimeTurnBindingRecord>> {
        self.cli_runtime_store()
            .list_cli_runtime_turn_binding_controls(filter)
            .await
    }

    pub async fn get_cli_runtime_turn_attempt(
        &self,
        id: &str,
    ) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
        self.cli_runtime_store()
            .get_cli_runtime_turn_attempt(id)
            .await
    }

    pub async fn get_cli_runtime_turn_attempt_by_native_turn(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
        self.cli_runtime_store()
            .get_cli_runtime_turn_attempt_by_native_turn(runtime_id, native_turn_id)
            .await
    }

    pub async fn get_cli_runtime_turn_attempt_by_recovery_attempt(
        &self,
        recovery_attempt_id: &str,
    ) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
        self.cli_runtime_store()
            .get_cli_runtime_turn_attempt_by_recovery_attempt(recovery_attempt_id)
            .await
    }

    pub async fn latest_cli_runtime_turn_attempt(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeTurnAttemptRecord>> {
        self.cli_runtime_store()
            .latest_cli_runtime_turn_attempt(turn_id)
            .await
    }

    pub async fn get_cli_runtime_execution_segment_by_native_turn(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
        self.cli_runtime_store()
            .get_cli_runtime_execution_segment_by_native_turn(runtime_id, native_turn_id)
            .await
    }

    pub async fn resolve_cli_runtime_native_turn_owner(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CliRuntimeNativeTurnOwner>> {
        self.cli_runtime_store()
            .resolve_cli_runtime_native_turn_owner(runtime_id, native_turn_id)
            .await
    }

    /// Native event ownership needs execution facts, not the provider input.
    /// Content consumers reload the mapping with its logical version fence.
    pub async fn resolve_cli_runtime_native_turn_control_owner(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
    ) -> Result<Option<CliRuntimeNativeTurnOwner>> {
        self.cli_runtime_store()
            .resolve_cli_runtime_native_turn_control_owner(runtime_id, native_turn_id)
            .await
    }

    pub async fn latest_cli_runtime_execution_segment_for_attempt(
        &self,
        attempt_id: &str,
    ) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
        self.cli_runtime_store()
            .latest_cli_runtime_execution_segment_for_attempt(attempt_id)
            .await
    }

    pub async fn latest_running_cli_runtime_execution_segment_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Option<CliRuntimeExecutionSegmentRecord>> {
        self.cli_runtime_store()
            .latest_running_cli_runtime_execution_segment_for_turn(turn_id)
            .await
    }

    pub async fn register_cli_runtime_execution_segment(
        &self,
        turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
        started_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<(
        CliRuntimeTurnBindingRecord,
        CliRuntimeTurnAttemptRecord,
        CliRuntimeExecutionSegmentRecord,
    )> {
        self.cli_runtime_store()
            .register_cli_runtime_execution_segment(
                turn_id,
                native_thread_id,
                native_turn_id,
                started_at,
            )
            .await
    }

    /// Control transition: the returned binding omits mapping bytes and read holds.
    pub async fn register_cli_runtime_control_execution_segment(
        &self,
        turn_id: &str,
        native_thread_id: &str,
        native_turn_id: &str,
        started_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<(
        CliRuntimeTurnBindingRecord,
        CliRuntimeTurnAttemptRecord,
        CliRuntimeExecutionSegmentRecord,
    )> {
        self.cli_runtime_store()
            .register_cli_runtime_control_execution_segment(
                turn_id,
                native_thread_id,
                native_turn_id,
                started_at,
            )
            .await
    }

    pub async fn terminalize_cli_runtime_execution_segment(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
        status: CliRuntimeExecutionSegmentStatus,
        failure_reason: Option<String>,
        completed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<
        Option<(
            CliRuntimeTurnBindingRecord,
            CliRuntimeTurnAttemptRecord,
            CliRuntimeExecutionSegmentRecord,
        )>,
    > {
        self.cli_runtime_store()
            .terminalize_cli_runtime_execution_segment(
                runtime_id,
                native_turn_id,
                status,
                failure_reason,
                completed_at,
            )
            .await
    }

    /// Control transition: the returned binding omits mapping bytes and read holds.
    pub async fn terminalize_cli_runtime_control_execution_segment(
        &self,
        runtime_id: &str,
        native_turn_id: &str,
        status: CliRuntimeExecutionSegmentStatus,
        failure_reason: Option<String>,
        completed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<
        Option<(
            CliRuntimeTurnBindingRecord,
            CliRuntimeTurnAttemptRecord,
            CliRuntimeExecutionSegmentRecord,
        )>,
    > {
        self.cli_runtime_store()
            .terminalize_cli_runtime_control_execution_segment(
                runtime_id,
                native_turn_id,
                status,
                failure_reason,
                completed_at,
            )
            .await
    }

    pub async fn mark_cli_runtime_turn_attempt_terminal(
        &self,
        id: &str,
        status: CliRuntimeTurnAttemptStatus,
        failure_reason: Option<String>,
        completed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<bool> {
        self.cli_runtime_store()
            .mark_cli_runtime_turn_attempt_terminal(id, status, failure_reason, completed_at)
            .await
    }

    pub async fn mark_cli_runtime_turn_attempt_recovery_confirmed(
        &self,
        id: &str,
        recovery_attempt_id: &str,
        confirmed_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
    ) -> Result<bool> {
        self.cli_runtime_store()
            .mark_cli_runtime_turn_attempt_recovery_confirmed(id, recovery_attempt_id, confirmed_at)
            .await
    }
}
