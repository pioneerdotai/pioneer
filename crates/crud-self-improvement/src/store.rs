//! Transaction owners for the self-improvement workspace and run lifecycle.

use std::{future::Future, time::Duration};

use anyhow::{Context, Result, bail};
use chrono::{TimeZone, Utc};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, PostgresDatabase,
    is_retryable_database_conflict, retry_with_backoff,
};
use pioneer_protocol::generate_id;
use sea_orm::TransactionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

use crate::{
    FinalizeSelfImprovementRunInput, FinalizeSelfImprovementRunResult, NewSelfImprovementRun,
    SelfImprovementFinalizationAuthority, SelfImprovementRunFence,
    SelfImprovementRunMutationResult, SelfImprovementRunRecord,
    SelfImprovementWorkspaceStateRecord, finalization, self_improvement_run,
    self_improvement_workspace_state, source_history,
};

const DB_ID_LEN: usize = 21;

#[derive(Clone)]
pub struct SelfImprovementStore {
    connection: PostgresDatabase,
}

impl SelfImprovementStore {
    pub fn new(connection: PostgresDatabase) -> Self {
        Self { connection }
    }

    pub fn with_maintenance_access(&self) -> Self {
        Self::new(self.connection.maintenance())
    }

    async fn retry_write_on_conflict<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry_with_backoff(
            operation,
            is_retryable_database_conflict,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub async fn get_self_improvement_workspace_state(
        &self,
        workspace_id: &str,
    ) -> Result<Option<SelfImprovementWorkspaceStateRecord>> {
        self_improvement_workspace_state::find(&self.connection, workspace_id)
            .await
            .map(|state| state.map(self_improvement_workspace_state::record_from_model))
    }

    pub async fn get_or_create_self_improvement_workspace_state(
        &self,
        workspace_id: &str,
        now_unix: i64,
    ) -> Result<SelfImprovementWorkspaceStateRecord> {
        let workspace_id = workspace_id.to_owned();
        self.retry_write_on_conflict(|| async {
            self_improvement_workspace_state::ensure(
                &self.connection,
                workspace_id.as_str(),
                unix_to_datetime(now_unix),
            )
            .await
            .map(self_improvement_workspace_state::record_from_model)
        })
        .await
    }

    pub async fn activate_self_improvement_workspace(
        &self,
        workspace_id: &str,
        effective_enabled_at_unix: i64,
    ) -> Result<SelfImprovementWorkspaceStateRecord> {
        let workspace_id = workspace_id.to_owned();
        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin self-improvement activation transaction")?;
            let effective_enabled_at = unix_to_datetime(effective_enabled_at_unix);

            if let Err(error) = self_improvement_workspace_state::ensure(
                &transaction,
                workspace_id.as_str(),
                effective_enabled_at,
            )
            .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }

            if let Err(error) = self_improvement_workspace_state::activate_if_inactive(
                &transaction,
                workspace_id.as_str(),
                effective_enabled_at,
            )
            .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }

            let state = match self_improvement_workspace_state::find(
                &transaction,
                workspace_id.as_str(),
            )
            .await
            {
                Ok(Some(state)) => state,
                Ok(None) => {
                    let _ = transaction.rollback().await;
                    bail!(
                        "self-improvement state disappeared during activation for workspace `{}`",
                        workspace_id
                    );
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };

            transaction
                .commit()
                .await
                .context("failed to commit self-improvement activation transaction")?;
            Ok(self_improvement_workspace_state::record_from_model(state))
        })
        .await
    }

    pub async fn deactivate_self_improvement_workspace(
        &self,
        workspace_id: &str,
        now_unix: i64,
    ) -> Result<SelfImprovementWorkspaceStateRecord> {
        let workspace_id = workspace_id.to_owned();
        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin self-improvement deactivation transaction")?;
            let now = unix_to_datetime(now_unix);

            if let Err(error) =
                self_improvement_workspace_state::ensure(&transaction, workspace_id.as_str(), now)
                    .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            if let Err(error) = self_improvement_workspace_state::deactivate_if_active(
                &transaction,
                workspace_id.as_str(),
                now,
            )
            .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            if let Err(error) = self_improvement_run::cancel_unfinished_for_workspace(
                &transaction,
                workspace_id.as_str(),
                "self_improvement_disabled",
                now,
            )
            .await
            {
                let _ = transaction.rollback().await;
                return Err(error);
            }
            let state = match self_improvement_workspace_state::find(
                &transaction,
                workspace_id.as_str(),
            )
            .await
            {
                Ok(Some(state)) => state,
                Ok(None) => {
                    let _ = transaction.rollback().await;
                    bail!(
                        "self-improvement state disappeared during deactivation for workspace `{}`",
                        workspace_id
                    );
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };

            transaction
                .commit()
                .await
                .context("failed to commit self-improvement deactivation transaction")?;
            Ok(self_improvement_workspace_state::record_from_model(state))
        })
        .await
    }

    pub async fn self_improvement_source_head(&self, workspace_id: &str) -> Result<i64> {
        source_history::source_head(&self.connection, workspace_id).await
    }

    pub async fn get_latest_self_improvement_run(
        &self,
        workspace_id: &str,
        activation_epoch: i64,
    ) -> Result<Option<SelfImprovementRunRecord>> {
        self_improvement_run::find_latest(&self.connection, workspace_id, activation_epoch)
            .await
            .map(|run| run.map(self_improvement_run::record_from_model))
    }

    pub async fn get_self_improvement_run(
        &self,
        workspace_id: &str,
        run_id: &str,
    ) -> Result<Option<SelfImprovementRunRecord>> {
        self_improvement_run::find_by_id(&self.connection, workspace_id, run_id)
            .await
            .map(|run| run.map(self_improvement_run::record_from_model))
    }

    pub async fn get_oldest_unresolved_self_improvement_run(
        &self,
        workspace_id: &str,
        activation_epoch: i64,
    ) -> Result<Option<SelfImprovementRunRecord>> {
        self_improvement_run::find_oldest_unresolved(
            &self.connection,
            workspace_id,
            activation_epoch,
        )
        .await
        .map(|run| run.map(self_improvement_run::record_from_model))
    }

    pub async fn get_next_self_improvement_retry_at(&self) -> Result<Option<i64>> {
        self_improvement_run::find_earliest_retry_at(&self.connection)
            .await
            .map(|retry_at| retry_at.map(|retry_at| retry_at.timestamp()))
    }

    pub async fn create_or_get_self_improvement_run(
        &self,
        input: NewSelfImprovementRun,
        now_unix: i64,
    ) -> Result<SelfImprovementRunRecord> {
        self_improvement_run::validate_new_run(&input)?;
        let run_id = generate_id(DB_ID_LEN);
        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin self-improvement run creation transaction")?;

            if let Some(existing) = self_improvement_run::find_daily(
                &transaction,
                input.workspace_id.as_str(),
                input.activation_epoch,
                input.scheduled_date_utc.as_str(),
            )
            .await?
            {
                transaction
                    .commit()
                    .await
                    .context("failed to close idempotent self-improvement run transaction")?;
                return Ok(self_improvement_run::record_from_model(existing));
            }

            if let Some(unresolved) = self_improvement_run::find_oldest_unresolved(
                &transaction,
                input.workspace_id.as_str(),
                input.activation_epoch,
            )
            .await?
            {
                let _ = transaction.rollback().await;
                bail!(
                    "self-improvement run `{}` must resolve before workspace `{}` can freeze a \
                     new source range",
                    unresolved.id,
                    input.workspace_id
                );
            }

            let state =
                self_improvement_workspace_state::find(&transaction, input.workspace_id.as_str())
                    .await?
                    .with_context(|| {
                        format!(
                            "self-improvement state is missing for workspace `{}`",
                            input.workspace_id
                        )
                    })?;
            if state.effective_enabled_at.is_none()
                || state.activation_epoch != input.activation_epoch
                || state.cursor_source_id != input.source_lower_exclusive
            {
                let _ = transaction.rollback().await;
                bail!(
                    "self-improvement run input is stale for workspace `{}`",
                    input.workspace_id
                );
            }

            let source_head =
                source_history::source_head(&transaction, input.workspace_id.as_str()).await?;
            if input.source_upper_inclusive > source_head {
                let _ = transaction.rollback().await;
                bail!(
                    "self-improvement run source upper `{}` exceeds workspace `{}` head `{}`",
                    input.source_upper_inclusive,
                    input.workspace_id,
                    source_head
                );
            }
            if !source_history::contains_source_id(
                &transaction,
                input.workspace_id.as_str(),
                input.source_upper_inclusive,
            )
            .await?
            {
                let _ = transaction.rollback().await;
                bail!(
                    "self-improvement run source upper `{}` is not a source in workspace `{}`",
                    input.source_upper_inclusive,
                    input.workspace_id
                );
            }

            self_improvement_run::insert_pending(
                &transaction,
                run_id.as_str(),
                &input,
                unix_to_datetime(now_unix),
            )
            .await?;
            let persisted = self_improvement_run::find_daily(
                &transaction,
                input.workspace_id.as_str(),
                input.activation_epoch,
                input.scheduled_date_utc.as_str(),
            )
            .await?
            .context("self-improvement run is missing after idempotent insert")?;

            transaction
                .commit()
                .await
                .context("failed to commit self-improvement run creation transaction")?;
            Ok(self_improvement_run::record_from_model(persisted))
        })
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
        if claimed_by.trim().is_empty() || claimed_by != claimed_by.trim() {
            bail!("self-improvement run claim owner must be non-empty and normalized");
        }
        if lease_expires_at_unix <= now_unix {
            bail!("self-improvement run lease must expire after claim time");
        }
        let workspace_id = workspace_id.to_owned();
        let run_id = run_id.to_owned();
        let claimed_by = claimed_by.to_owned();
        let claim_token = generate_id(32);

        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin self-improvement run claim transaction")?;
            let run = self_improvement_run::find_by_id(
                &transaction,
                workspace_id.as_str(),
                run_id.as_str(),
            )
            .await?;
            let Some(run) = run else {
                transaction
                    .commit()
                    .await
                    .context("failed to close missing self-improvement run claim transaction")?;
                return Ok(None);
            };
            let state =
                self_improvement_workspace_state::find(&transaction, workspace_id.as_str()).await?;
            let state_matches = state.is_some_and(|state| {
                state.effective_enabled_at.is_some()
                    && state.activation_epoch == activation_epoch
                    && state.cursor_source_id == run.source_lower_exclusive
            });
            if !state_matches {
                transaction
                    .commit()
                    .await
                    .context("failed to close stale self-improvement run claim transaction")?;
                return Ok(None);
            }

            let claimed = self_improvement_run::claim_available(
                &transaction,
                workspace_id.as_str(),
                run_id.as_str(),
                activation_epoch,
                claim_token.as_str(),
                claimed_by.as_str(),
                unix_to_datetime(now_unix),
                unix_to_datetime(lease_expires_at_unix),
            )
            .await?;
            let claimed_run = if claimed {
                self_improvement_run::find_by_id(
                    &transaction,
                    workspace_id.as_str(),
                    run_id.as_str(),
                )
                .await?
                .map(self_improvement_run::record_from_model)
            } else {
                None
            };

            transaction
                .commit()
                .await
                .context("failed to commit self-improvement run claim transaction")?;
            Ok(claimed_run)
        })
        .await
    }

    pub async fn save_self_improvement_run_checkpoint(
        &self,
        fence: &SelfImprovementRunFence,
        analysis_cursor_json: &str,
        analysis_digest_json: &str,
        now_unix: i64,
    ) -> Result<SelfImprovementRunMutationResult> {
        let fence = fence.clone();
        let analysis_cursor_json = analysis_cursor_json.to_owned();
        let analysis_digest_json = analysis_digest_json.to_owned();
        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin self-improvement checkpoint transaction")?;
            let applied = self_improvement_run::save_checkpoint(
                &transaction,
                &fence,
                analysis_cursor_json.as_str(),
                analysis_digest_json.as_str(),
                unix_to_datetime(now_unix),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit self-improvement checkpoint transaction")?;
            Ok(applied)
        })
        .await
    }

    pub async fn heartbeat_self_improvement_run(
        &self,
        fence: &SelfImprovementRunFence,
        now_unix: i64,
        lease_expires_at_unix: i64,
    ) -> Result<SelfImprovementRunMutationResult> {
        if lease_expires_at_unix <= now_unix {
            bail!("self-improvement heartbeat lease must expire after heartbeat time");
        }
        let fence = fence.clone();
        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin self-improvement heartbeat transaction")?;
            let result = self_improvement_run::heartbeat(
                &transaction,
                &fence,
                unix_to_datetime(now_unix),
                unix_to_datetime(lease_expires_at_unix),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit self-improvement heartbeat transaction")?;
            Ok(result)
        })
        .await
    }

    pub async fn requeue_failed_self_improvement_run(
        &self,
        run: &SelfImprovementRunRecord,
        now_unix: i64,
    ) -> Result<SelfImprovementRunMutationResult> {
        let run = run.clone();
        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin failed self-improvement run requeue transaction")?;
            let result = self_improvement_run::requeue_failed(
                &transaction,
                &run,
                unix_to_datetime(now_unix),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit failed self-improvement run requeue transaction")?;
            Ok(result)
        })
        .await
    }

    pub async fn reset_unfinished_self_improvement_run_authority(
        &self,
        run: &SelfImprovementRunRecord,
        authority: &SelfImprovementFinalizationAuthority,
        now_unix: i64,
    ) -> Result<SelfImprovementRunMutationResult> {
        let run = run.clone();
        let authority = authority.clone();
        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin self-improvement run authority reset transaction")?;
            let result = self_improvement_run::reset_unfinished_authority(
                &transaction,
                &run,
                &authority,
                unix_to_datetime(now_unix),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit self-improvement run authority reset transaction")?;
            Ok(result)
        })
        .await
    }

    pub async fn return_self_improvement_run_to_pending(
        &self,
        fence: &SelfImprovementRunFence,
        now_unix: i64,
        next_attempt_at_unix: i64,
        last_error: &str,
    ) -> Result<SelfImprovementRunMutationResult> {
        if next_attempt_at_unix <= now_unix {
            bail!("self-improvement retry must be scheduled after transition time");
        }
        self.transition_claimed_self_improvement_run(
            fence,
            self_improvement_run::STATUS_PENDING,
            Some(next_attempt_at_unix),
            last_error,
            now_unix,
        )
        .await
    }

    pub async fn yield_self_improvement_run_after_budget(
        &self,
        fence: &SelfImprovementRunFence,
        now_unix: i64,
        next_attempt_at_unix: i64,
    ) -> Result<SelfImprovementRunMutationResult> {
        if next_attempt_at_unix <= now_unix {
            bail!("self-improvement budget yield must schedule a future wake");
        }
        let fence = fence.clone();
        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin self-improvement budget-yield transaction")?;
            let result = self_improvement_run::yield_claimed(
                &transaction,
                &fence,
                unix_to_datetime(next_attempt_at_unix),
                unix_to_datetime(now_unix),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit self-improvement budget-yield transaction")?;
            Ok(result)
        })
        .await
    }

    pub async fn fail_claimed_self_improvement_run(
        &self,
        fence: &SelfImprovementRunFence,
        now_unix: i64,
        last_error: &str,
    ) -> Result<SelfImprovementRunMutationResult> {
        self.transition_claimed_self_improvement_run(
            fence,
            self_improvement_run::STATUS_FAILED,
            None,
            last_error,
            now_unix,
        )
        .await
    }

    pub async fn cancel_claimed_self_improvement_run(
        &self,
        fence: &SelfImprovementRunFence,
        now_unix: i64,
        reason: &str,
    ) -> Result<SelfImprovementRunMutationResult> {
        self.transition_claimed_self_improvement_run(
            fence,
            self_improvement_run::STATUS_CANCELLED,
            None,
            reason,
            now_unix,
        )
        .await
    }

    async fn transition_claimed_self_improvement_run(
        &self,
        fence: &SelfImprovementRunFence,
        status: &str,
        next_attempt_at_unix: Option<i64>,
        last_error: &str,
        now_unix: i64,
    ) -> Result<SelfImprovementRunMutationResult> {
        let fence = fence.clone();
        let status = status.to_owned();
        let last_error = last_error.to_owned();
        self.retry_write_on_conflict(|| async {
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin self-improvement run transition transaction")?;
            let result = self_improvement_run::transition_claimed(
                &transaction,
                &fence,
                status.as_str(),
                next_attempt_at_unix.map(unix_to_datetime),
                last_error.as_str(),
                unix_to_datetime(now_unix),
            )
            .await?;
            transaction
                .commit()
                .await
                .context("failed to commit self-improvement run transition transaction")?;
            Ok(result)
        })
        .await
    }

    pub async fn finalize_self_improvement_run(
        &self,
        input: FinalizeSelfImprovementRunInput,
        now_unix: i64,
    ) -> Result<FinalizeSelfImprovementRunResult> {
        let prepared = finalization::prepare(&self.connection, input).await?;
        self.retry_write_on_conflict(|| {
            let prepared = prepared.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin self-improvement finalization transaction")?;
                let result =
                    finalization::finalize(&transaction, prepared, unix_to_datetime(now_unix))
                        .await;
                let result = match result {
                    Ok(result) => result,
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        return Err(error);
                    }
                };
                transaction
                    .commit()
                    .await
                    .context("failed to commit self-improvement finalization transaction")?;
                Ok(result)
            }
        })
        .await
    }
}

fn unix_to_datetime(value: i64) -> DateTimeWithTimeZone {
    Utc.timestamp_opt(value, 0)
        .single()
        .unwrap_or_else(Utc::now)
        .fixed_offset()
}

use crate::SelfImprovementSourceTurnRecord;
use crate::canonical_history::SelfImprovementFrozenSourceRange;
use pioneer_crud_conversations::canonical_turn_event::CanonicalTurnEventRecord;
use pioneer_crud_packages::agent_skill::AgentSkillVersionSnapshotRecord;

impl SelfImprovementStore {
    pub async fn reconcile_self_improvement_history_cursor(
        &self,
        workspace_id: &str,
    ) -> Result<()> {
        let maintenance = self.with_maintenance_access();
        let Some(state) =
            crate::self_improvement_workspace_state::find(&maintenance.connection, workspace_id)
                .await?
        else {
            return Ok(());
        };
        if state.effective_enabled_at.is_none() || state.cursor_source_id == 0 {
            return Ok(());
        }
        let sources =
            crate::source_turn::list_after_cursor(&maintenance.connection, workspace_id, 0, 0, 1)
                .await?;
        let Some(earliest) = sources
            .first()
            .map(|source| source.id)
            .filter(|id| *id <= state.cursor_source_id)
        else {
            return Ok(());
        };
        maintenance
            .retry_write_on_conflict(|| async {
                let transaction = maintenance.connection.begin().await?;
                let result =
                    crate::history_cursor::rewind_idle(&transaction, &state, earliest).await;
                match result {
                    Ok(()) => {
                        transaction.commit().await?;
                        Ok(())
                    }
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        Err(error)
                    }
                }
            })
            .await
    }

    pub async fn list_self_improvement_source_turns_after(
        &self,
        workspace_id: &str,
        cursor_source_id: i64,
        effective_enabled_at_unix: i64,
        limit: u64,
    ) -> Result<Vec<SelfImprovementSourceTurnRecord>> {
        crate::source_turn::list_after_cursor(
            &self.connection,
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
        crate::source_turn::list_frozen_range(
            &self.connection,
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
        crate::canonical_history::list_for_frozen_range(&self.connection, frozen_range).await
    }

    pub async fn self_improvement_processed_history_date(
        &self,
        workspace_id: &str,
    ) -> Result<Option<i64>> {
        crate::source_turn::processed_history_date(&self.connection, workspace_id).await
    }

    pub async fn recent_processed_self_improvement_sources(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<SelfImprovementSourceTurnRecord>> {
        crate::source_turn::recent_processed_sources(&self.connection, workspace_id).await
    }

    pub async fn list_active_agent_skill_versions(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<AgentSkillVersionSnapshotRecord>> {
        pioneer_crud_packages::agent_skill::list_active_versions(&self.connection, workspace_id)
            .await
    }
}
