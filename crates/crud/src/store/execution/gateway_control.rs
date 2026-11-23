use crate::{CrudStore, RecoveryJobRecord, TurnExecutionRecord};
use anyhow::Result;
use pioneer_crud_execution::gateway_control::GatewayControlRepository;
pub use pioneer_crud_execution::gateway_control::{GatewayControlConnection, GatewayControlWake};
use pioneer_crud_packages::skill_upload_session::next_skill_upload_deadline;
impl CrudStore {
    pub async fn control_turn_item_attempt_upper_id(&self) -> Result<Option<String>> {
        GatewayControlRepository::new(&self.connection)
            .control_turn_item_attempt_upper_id()
            .await
    }

    /// Snapshot membership ceilings once per pass. Eligibility is revalidated

    /// by every page and mutation; no snapshot or DB capacity is retained.

    pub async fn control_turn_execution_upper_id(&self) -> Result<Option<String>> {
        GatewayControlRepository::new(&self.connection)
            .control_turn_execution_upper_id()
            .await
    }

    pub async fn control_recovery_job_upper_id(&self) -> Result<Option<String>> {
        GatewayControlRepository::new(&self.connection)
            .control_recovery_job_upper_id()
            .await
    }

    pub async fn list_expired_foreign_turn_execution_page(
        &self,
        owner: &str,
        now: i64,
        after: Option<&str>,
        upper: &str,
        limit: u64,
    ) -> Result<(Vec<Result<TurnExecutionRecord>>, Option<String>)> {
        GatewayControlRepository::new(&self.connection)
            .list_expired_foreign_turn_execution_page(owner, now, after, upper, limit)
            .await
    }

    pub async fn list_due_terminal_repair_page(
        &self,
        now: i64,
        limit: u64,
        after: Option<&str>,
        upper: &str,
    ) -> Result<(Vec<RecoveryJobRecord>, Option<String>)> {
        GatewayControlRepository::new(&self.connection)
            .list_due_terminal_repair_page(now, limit, after, upper)
            .await
    }

    /// A finite orphan repair sweep must advance through idempotent rows even

    /// when enqueueing their already existing job emitted no domain event.

    pub async fn list_owned_recovering_turn_execution_page(
        &self,
        owner: &str,
        after: Option<&str>,
        upper: &str,
        limit: u64,
    ) -> Result<(Vec<Result<TurnExecutionRecord>>, Option<String>)> {
        GatewayControlRepository::new(&self.connection)
            .list_owned_recovering_turn_execution_page(owner, after, upper, limit)
            .await
    }

    /// Future heads after draining source work. Combine with the coordinator's

    /// next_supervision_deadline(now), next_skill_cleanup_deadline(now),

    /// CLI/RAM deadlines and retained failures.

    /// Full input quanta/cursors continue immediately, never by a safety timer.

    pub async fn next_gateway_control_deadline(&self, now: i64) -> Result<Option<i64>> {
        let execution = GatewayControlRepository::new(&self.connection)
            .next_gateway_control_deadline(now)
            .await?;
        Ok(execution
            .into_iter()
            .chain(next_skill_upload_deadline(&self.connection, now).await?)
            .min())
    }

    /// Query only the heads belonging to an invalidated or due control family.

    pub async fn next_gateway_control_deadline_for(
        &self,
        scope: &GatewayControlWake,
        now: i64,
    ) -> Result<Option<i64>> {
        if matches!(scope, GatewayControlWake::Skills) {
            return next_skill_upload_deadline(&self.connection, now).await;
        }
        GatewayControlRepository::new(&self.connection)
            .next_gateway_control_deadline_for(scope, now)
            .await
    }
}

impl CrudStore {
    /// Call before dispatch; complete only after commit and resource release.
    /// Other consumers of a multi-domain publisher retain their old protocol.
    pub(crate) fn control_work_change(
        &self,
        domain: &'static str,
        work: &[GatewayControlWake],
    ) -> pioneer_postgres_pools::WorkChange {
        pioneer_crud_execution::gateway_control::control_work_change(&self.connection, domain, work)
    }
}
