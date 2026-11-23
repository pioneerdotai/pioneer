use crate::CrudStore;
use anyhow::Result;
use pioneer_crud_execution::execution_observability::{
    ExecutionObservationRepository, PreparedExecutionObservation,
};
use pioneer_crud_execution::provider_usage::ProviderUsageObservation;
use pioneer_entity::{
    execution_observation as fact, execution_tariff as tariff,
    provider_usage_observation as journal,
};
use serde_json::Value;

impl CrudStore {
    pub async fn record_execution_observation(
        &self,
        prepared: PreparedExecutionObservation,
    ) -> Result<()> {
        ExecutionObservationRepository::new(&self.connection)
            .record_execution_observation(prepared)
            .await
    }

    pub async fn execution_observation_frontier(
        &self,
        workspace: &str,
        thread: &str,
    ) -> Result<i64> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_observation_frontier(workspace, thread)
            .await
    }

    pub async fn execution_workspace_frontier(&self, workspace: &str) -> Result<i64> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_workspace_frontier(workspace)
            .await
    }

    pub async fn execution_observation_page(
        &self,
        workspace: &str,
        thread: &str,
        after: i64,
        through: i64,
        limit: u64,
        latest_only: bool,
    ) -> Result<Vec<fact::Model>> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_observation_page(workspace, thread, after, through, limit, latest_only)
            .await
    }

    pub async fn workspace_owned_execution_observation_page(
        &self,
        workspace: &str,
        after: i64,
        through: i64,
        limit: u64,
    ) -> Result<Vec<fact::Model>> {
        ExecutionObservationRepository::new(&self.connection)
            .workspace_owned_execution_observation_page(workspace, after, through, limit)
            .await
    }

    pub async fn execution_observation_latest(
        &self,
        workspace: &str,
        thread: &str,
        operation: &str,
    ) -> Result<Option<fact::Model>> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_observation_latest(workspace, thread, operation)
            .await
    }

    pub async fn workspace_owned_execution_observation_latest(
        &self,
        workspace: &str,
        operation: &str,
    ) -> Result<Option<fact::Model>> {
        ExecutionObservationRepository::new(&self.connection)
            .workspace_owned_execution_observation_latest(workspace, operation)
            .await
    }

    pub async fn execution_task_ancestry_matches(
        &self,
        workspace: &str,
        candidate: &str,
        ancestor: &str,
    ) -> Result<Option<bool>> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_task_ancestry_matches(workspace, candidate, ancestor)
            .await
    }

    pub async fn execution_turn_owned_by(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        execution: &str,
    ) -> Result<bool> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_turn_owned_by(workspace, thread, turn, execution)
            .await
    }

    pub async fn execution_children_page(
        &self,
        workspace: &str,
        thread: Option<&str>,
        parent: &str,
        after: i64,
        through: i64,
        limit: u64,
    ) -> Result<Vec<fact::Model>> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_children_page(workspace, thread, parent, after, through, limit)
            .await
    }

    pub async fn execution_operation_at_frontier(
        &self,
        workspace: &str,
        thread: Option<&str>,
        operation: &str,
        through: i64,
    ) -> Result<Option<fact::Model>> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_operation_at_frontier(workspace, thread, operation, through)
            .await
    }

    pub async fn execution_tariff(
        &self,
        workspace: &str,
        hash: &str,
    ) -> Result<Option<tariff::Model>> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_tariff(workspace, hash)
            .await
    }

    pub async fn record_provider_phase(
        &self,
        workspace: &str,
        owner: &str,
        facts: Value,
    ) -> Result<()> {
        ExecutionObservationRepository::new(&self.connection)
            .record_provider_phase(workspace, owner, facts)
            .await
    }

    pub async fn execution_recent_owner_turns(
        &self,
        workspace: &str,
        thread_id: &str,
        task_id: Option<&str>,
        execution_id: Option<&str>,
    ) -> Result<Vec<String>> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_recent_owner_turns(workspace, thread_id, task_id, execution_id)
            .await
    }

    pub async fn execution_scope_owners(
        &self,
        workspace: &str,
        thread: &str,
        turns: Vec<String>,
    ) -> Result<Vec<Value>> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_scope_owners(workspace, thread, turns)
            .await
    }

    pub(crate) async fn prepare_retained_tool_source(
        &self,
        entry: &crate::NewTurnLlmContextEntry,
        delivery_key: &str,
    ) -> Result<Option<PreparedExecutionObservation>> {
        ExecutionObservationRepository::new(&self.connection)
            .prepare_retained_tool_source(entry, delivery_key)
            .await
    }

    pub async fn record_provider_usage(&self, record: ProviderUsageObservation) -> Result<()> {
        ExecutionObservationRepository::new(&self.connection)
            .record_provider_usage(record)
            .await
    }

    pub async fn provider_usage_page(
        &self,
        workspace: &str,
        owner: &str,
        after: &str,
        limit: u64,
    ) -> Result<Vec<ProviderUsageObservation>> {
        ExecutionObservationRepository::new(&self.connection)
            .provider_usage_page(workspace, owner, after, limit)
            .await
    }

    pub async fn record_native_provider_usage(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
        id: &str,
        revision: i64,
        usage: serde_json::Value,
    ) -> Result<()> {
        ExecutionObservationRepository::new(&self.connection)
            .record_native_provider_usage(workspace, thread, turn, id, revision, usage)
            .await
    }

    pub async fn execution_live_usage(
        &self,
        workspace: &str,
        thread: &str,
        ids: Vec<String>,
    ) -> Result<Vec<journal::Model>> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_live_usage(workspace, thread, ids)
            .await
    }

    pub async fn execution_scoped_live_usage(
        &self,
        workspace: &str,
        owner: Option<&str>,
        ids: Vec<String>,
    ) -> Result<Vec<journal::Model>> {
        ExecutionObservationRepository::new(&self.connection)
            .execution_scoped_live_usage(workspace, owner, ids)
            .await
    }
}
