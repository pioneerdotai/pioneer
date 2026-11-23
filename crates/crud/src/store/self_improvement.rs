use crate::{
    AgentSkillVersionSnapshotRecord, CanonicalTurnEventRecord, CrudStore,
    FinalizeSelfImprovementRunInput, FinalizeSelfImprovementRunResult, NewSelfImprovementRun,
    SelfImprovementFinalizationAuthority, SelfImprovementFrozenSourceRange,
    SelfImprovementRunFence, SelfImprovementRunMutationResult, SelfImprovementRunRecord,
    SelfImprovementSourceTurnRecord, SelfImprovementWorkspaceStateRecord,
};
use anyhow::Result;
use pioneer_protocol::SkillId;

impl CrudStore {
    pub async fn list_active_agent_skill_versions(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<AgentSkillVersionSnapshotRecord>> {
        self.self_improvement_store()
            .list_active_agent_skill_versions(workspace_id)
            .await
    }

    pub async fn self_improvement_processed_history_date(
        &self,
        workspace_id: &str,
    ) -> Result<Option<i64>> {
        self.self_improvement_store()
            .self_improvement_processed_history_date(workspace_id)
            .await
    }

    pub async fn recent_processed_self_improvement_sources(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<SelfImprovementSourceTurnRecord>> {
        self.self_improvement_store()
            .recent_processed_self_improvement_sources(workspace_id)
            .await
    }

    pub async fn get_agent_skill_version(
        &self,
        workspace_id: &str,
        version_id: &str,
    ) -> Result<Option<AgentSkillVersionSnapshotRecord>> {
        pioneer_crud_packages::agent_skill::find_exact_version(
            &self.connection,
            workspace_id,
            version_id,
        )
        .await
    }

    pub async fn get_next_agent_skill_version_number(
        &self,
        workspace_id: &str,
        skill_id: &SkillId,
    ) -> Result<Option<i64>> {
        pioneer_crud_packages::agent_skill::find_next_version_number(
            &self.connection,
            workspace_id,
            skill_id,
        )
        .await
    }

    pub async fn list_agent_skill_version_fingerprints(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<String>> {
        pioneer_crud_packages::agent_skill::list_workspace_version_fingerprints(
            &self.connection,
            workspace_id,
        )
        .await
    }

    fn self_improvement_store(&self) -> pioneer_crud_self_improvement::store::SelfImprovementStore {
        pioneer_crud_self_improvement::store::SelfImprovementStore::new(self.connection.clone())
    }

    pub async fn get_self_improvement_workspace_state(
        &self,
        workspace_id: &str,
    ) -> Result<Option<SelfImprovementWorkspaceStateRecord>> {
        self.self_improvement_store()
            .get_self_improvement_workspace_state(workspace_id)
            .await
    }

    pub async fn get_or_create_self_improvement_workspace_state(
        &self,
        workspace_id: &str,
        now_unix: i64,
    ) -> Result<SelfImprovementWorkspaceStateRecord> {
        self.self_improvement_store()
            .get_or_create_self_improvement_workspace_state(workspace_id, now_unix)
            .await
    }

    pub async fn activate_self_improvement_workspace(
        &self,
        workspace_id: &str,
        effective_enabled_at_unix: i64,
    ) -> Result<SelfImprovementWorkspaceStateRecord> {
        self.self_improvement_store()
            .activate_self_improvement_workspace(workspace_id, effective_enabled_at_unix)
            .await
    }

    pub async fn deactivate_self_improvement_workspace(
        &self,
        workspace_id: &str,
        now_unix: i64,
    ) -> Result<SelfImprovementWorkspaceStateRecord> {
        self.self_improvement_store()
            .deactivate_self_improvement_workspace(workspace_id, now_unix)
            .await
    }

    pub async fn self_improvement_source_head(&self, workspace_id: &str) -> Result<i64> {
        self.self_improvement_store()
            .self_improvement_source_head(workspace_id)
            .await
    }

    /// Revisit a source that became visible behind the current cursor. An
    /// unresolved run retains its frozen bounds and checkpoint.
    pub async fn reconcile_self_improvement_history_cursor(
        &self,
        workspace_id: &str,
    ) -> Result<()> {
        self.self_improvement_store()
            .reconcile_self_improvement_history_cursor(workspace_id)
            .await
    }

    pub async fn list_self_improvement_source_turns_after(
        &self,
        workspace_id: &str,
        cursor_source_id: i64,
        effective_enabled_at_unix: i64,
        limit: u64,
    ) -> Result<Vec<SelfImprovementSourceTurnRecord>> {
        self.self_improvement_store()
            .list_self_improvement_source_turns_after(
                workspace_id,
                cursor_source_id,
                effective_enabled_at_unix,
                limit,
            )
            .await
    }

    pub async fn list_frozen_self_improvement_source_range(
        &self,
        workspace_id: &str,
        source_lower_exclusive: i64,
        source_upper_inclusive: i64,
        effective_enabled_at_unix: i64,
    ) -> Result<Vec<SelfImprovementSourceTurnRecord>> {
        self.self_improvement_store()
            .list_frozen_self_improvement_source_range(
                workspace_id,
                source_lower_exclusive,
                source_upper_inclusive,
                effective_enabled_at_unix,
            )
            .await
    }

    pub async fn list_canonical_turn_events_for_self_improvement(
        &self,
        frozen_range: &SelfImprovementFrozenSourceRange,
    ) -> Result<Vec<CanonicalTurnEventRecord>> {
        self.self_improvement_store()
            .list_canonical_turn_events_for_self_improvement(frozen_range)
            .await
    }

    /// Read-only, workspace/activation-scoped summary source for the settings surface.
    pub async fn get_latest_self_improvement_run(
        &self,
        workspace_id: &str,
        activation_epoch: i64,
    ) -> Result<Option<SelfImprovementRunRecord>> {
        self.self_improvement_store()
            .get_latest_self_improvement_run(workspace_id, activation_epoch)
            .await
    }

    pub async fn get_self_improvement_run(
        &self,
        workspace_id: &str,
        run_id: &str,
    ) -> Result<Option<SelfImprovementRunRecord>> {
        self.self_improvement_store()
            .get_self_improvement_run(workspace_id, run_id)
            .await
    }

    pub async fn get_oldest_unresolved_self_improvement_run(
        &self,
        workspace_id: &str,
        activation_epoch: i64,
    ) -> Result<Option<SelfImprovementRunRecord>> {
        self.self_improvement_store()
            .get_oldest_unresolved_self_improvement_run(workspace_id, activation_epoch)
            .await
    }

    pub async fn get_next_self_improvement_retry_at(&self) -> Result<Option<i64>> {
        self.self_improvement_store()
            .get_next_self_improvement_retry_at()
            .await
    }

    pub async fn create_or_get_self_improvement_run(
        &self,
        input: NewSelfImprovementRun,
        now_unix: i64,
    ) -> Result<SelfImprovementRunRecord> {
        self.self_improvement_store()
            .create_or_get_self_improvement_run(input, now_unix)
            .await
    }

    pub async fn claim_available_self_improvement_run(
        &self,
        workspace_id: &str,
        run_id: &str,
        activation_epoch: i64,
        claimed_by: &str,
        now_unix: i64,
        lease_expires_at_unix: i64,
    ) -> Result<Option<SelfImprovementRunRecord>> {
        self.self_improvement_store()
            .claim_available_self_improvement_run(
                workspace_id,
                run_id,
                activation_epoch,
                claimed_by,
                now_unix,
                lease_expires_at_unix,
            )
            .await
    }

    pub async fn save_self_improvement_run_checkpoint(
        &self,
        fence: &SelfImprovementRunFence,
        analysis_cursor_json: &str,
        analysis_digest_json: &str,
        now_unix: i64,
    ) -> Result<SelfImprovementRunMutationResult> {
        self.self_improvement_store()
            .save_self_improvement_run_checkpoint(
                fence,
                analysis_cursor_json,
                analysis_digest_json,
                now_unix,
            )
            .await
    }

    pub async fn heartbeat_self_improvement_run(
        &self,
        fence: &SelfImprovementRunFence,
        now_unix: i64,
        lease_expires_at_unix: i64,
    ) -> Result<SelfImprovementRunMutationResult> {
        self.self_improvement_store()
            .heartbeat_self_improvement_run(fence, now_unix, lease_expires_at_unix)
            .await
    }

    pub async fn requeue_failed_self_improvement_run(
        &self,
        run: &SelfImprovementRunRecord,
        now_unix: i64,
    ) -> Result<SelfImprovementRunMutationResult> {
        self.self_improvement_store()
            .requeue_failed_self_improvement_run(run, now_unix)
            .await
    }

    pub async fn reset_unfinished_self_improvement_run_authority(
        &self,
        run: &SelfImprovementRunRecord,
        authority: &SelfImprovementFinalizationAuthority,
        now_unix: i64,
    ) -> Result<SelfImprovementRunMutationResult> {
        self.self_improvement_store()
            .reset_unfinished_self_improvement_run_authority(run, authority, now_unix)
            .await
    }

    pub async fn return_self_improvement_run_to_pending(
        &self,
        fence: &SelfImprovementRunFence,
        now_unix: i64,
        next_attempt_at_unix: i64,
        last_error: &str,
    ) -> Result<SelfImprovementRunMutationResult> {
        self.self_improvement_store()
            .return_self_improvement_run_to_pending(
                fence,
                now_unix,
                next_attempt_at_unix,
                last_error,
            )
            .await
    }

    pub async fn yield_self_improvement_run_after_budget(
        &self,
        fence: &SelfImprovementRunFence,
        now_unix: i64,
        next_attempt_at_unix: i64,
    ) -> Result<SelfImprovementRunMutationResult> {
        self.self_improvement_store()
            .yield_self_improvement_run_after_budget(fence, now_unix, next_attempt_at_unix)
            .await
    }

    pub async fn fail_claimed_self_improvement_run(
        &self,
        fence: &SelfImprovementRunFence,
        now_unix: i64,
        last_error: &str,
    ) -> Result<SelfImprovementRunMutationResult> {
        self.self_improvement_store()
            .fail_claimed_self_improvement_run(fence, now_unix, last_error)
            .await
    }

    pub async fn cancel_claimed_self_improvement_run(
        &self,
        fence: &SelfImprovementRunFence,
        now_unix: i64,
        reason: &str,
    ) -> Result<SelfImprovementRunMutationResult> {
        self.self_improvement_store()
            .cancel_claimed_self_improvement_run(fence, now_unix, reason)
            .await
    }

    pub async fn finalize_self_improvement_run(
        &self,
        input: FinalizeSelfImprovementRunInput,
        now_unix: i64,
    ) -> Result<FinalizeSelfImprovementRunResult> {
        self.self_improvement_store()
            .finalize_self_improvement_run(input, now_unix)
            .await
    }
}
