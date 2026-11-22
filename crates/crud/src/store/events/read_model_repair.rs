use crate::{
    CrudStore, ReadModelInvariantViolation, ReadModelRepairReport, ReadModelRepairStage,
    RepairSummary,
};
use anyhow::Result;
use pioneer_crud_execution::read_model_repair::ReadModelRepairTimings;

impl CrudStore {
    pub async fn list_read_model_invariant_violations(
        &self,
    ) -> Result<Vec<ReadModelInvariantViolation>> {
        let mut violations =
            pioneer_crud_execution::read_model_repair::list_execution_invariant_violations(
                &self.connection,
            )
            .await?;
        violations.extend(
            pioneer_crud_tasks::read_model_repair::list_task_invariant_violations(&self.connection)
                .await?,
        );
        Ok(violations)
    }

    pub async fn repair_deterministic_read_model_violations(&self) -> Result<RepairSummary> {
        Ok(self
            .repair_deterministic_read_model_violations_with_timings()
            .await?
            .summary)
    }

    pub async fn repair_deterministic_read_model_violations_with_timings(
        &self,
    ) -> Result<ReadModelRepairReport> {
        let maintenance = self.with_maintenance_access();
        let mut timings = ReadModelRepairTimings::default();
        let mut summary = RepairSummary::default();
        summary.merge(
            maintenance
                .repair_terminal_turn_item_payloads(&mut timings)
                .await?,
        );
        summary.merge(
            timings
                .observe(
                    ReadModelRepairStage::TerminalTurnRunningAttempts,
                    maintenance.repair_terminal_turn_running_attempts(),
                )
                .await?,
        );
        summary.merge(
            timings
                .observe(
                    ReadModelRepairStage::TerminalTasksMissingCompletedAt,
                    maintenance.repair_terminal_tasks_missing_completed_at(),
                )
                .await?,
        );
        summary.merge(
            timings
                .observe(
                    ReadModelRepairStage::TerminalRunsMissingCompletedAt,
                    maintenance.repair_terminal_runs_missing_completed_at(),
                )
                .await?,
        );
        Ok(ReadModelRepairReport {
            summary,
            stage_timings: timings.into_stage_timings(),
        })
    }

    async fn repair_terminal_turn_item_payloads(
        &self,
        timings: &mut ReadModelRepairTimings,
    ) -> Result<RepairSummary> {
        self.execution_read_model_repair()
            .repair_terminal_turn_item_payloads(timings)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn apply_one_terminal_turn_item_payload_repair(
        &self,
        transaction: &sea_orm::DatabaseTransaction,
        row: &pioneer_entity::turn_item::Model,
        payload: &pioneer_crud_payloads::storage::PreparedPayload,
        episodic: Option<&pioneer_crud_memory::episodic_source_store::PreparedSourceSave>,
    ) -> Result<bool> {
        self.execution_read_model_repair()
            .apply_one_terminal_turn_item_payload_repair(transaction, row, payload, episodic)
            .await
    }

    async fn repair_terminal_turn_running_attempts(&self) -> Result<RepairSummary> {
        self.execution_read_model_repair()
            .repair_terminal_turn_running_attempts()
            .await
    }

    async fn repair_terminal_tasks_missing_completed_at(&self) -> Result<RepairSummary> {
        self.task_event_store()
            .repair_terminal_tasks_missing_completed_at()
            .await
    }

    async fn repair_terminal_runs_missing_completed_at(&self) -> Result<RepairSummary> {
        self.task_event_store()
            .repair_terminal_runs_missing_completed_at()
            .await
    }
}
