use crate::store::execution::agent_transactions;
use crate::{
    AgentDelegationRouteInput, AgentExecutionGraphCommitInput, AgentExecutionGraphCommitResult,
    AgentWorkGraphCancellationTarget, CrudStore, GatewayControlWake, PromotedAgentExecution,
    TaskRunExecutionObservationRecord, TurnExecutionRecord, TurnExecutionStatus,
    cancel_agent_work_graph, commit_agent_execution_graph, finalize_agent_execution,
    insert_agent_delegation_route, load_agent_turn_response, promote_queued_agent_executions,
    revoke_agent_delegation_route,
};
use anyhow::{Context, Result};
use pioneer_crud_access::turn_execution::turn_status_from_db;
use pioneer_crud_execution::{agent_delegation_route, turn_execution};
use pioneer_protocol::{
    Task, TaskError, TaskResult, TaskRunExecution, TaskRunExecutionStatus, Turn, TurnStatus,
};
use sea_orm::TransactionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

impl CrudStore {
    pub async fn create_agent_delegation_route(
        &self,
        input: AgentDelegationRouteInput,
    ) -> Result<pioneer_entity::agent_delegation_route::Model> {
        self.run_serialized_write(|| agent_delegation_route::create_route(&self.connection, &input))
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn revoke_agent_delegation_route(
        &self,
        route_id: &str,
        expected_generation: i64,
        expected_policy_generation: i64,
        authority_actor_json: &str,
        authority_fingerprint: &str,
        now: DateTimeWithTimeZone,
    ) -> Result<bool> {
        self.run_serialized_write(|| {
            agent_delegation_route::revoke_route(
                &self.connection,
                route_id,
                expected_generation,
                expected_policy_generation,
                authority_actor_json,
                authority_fingerprint,
                now,
            )
        })
        .await
    }

    pub async fn expire_agent_delegation_routes(&self, now: DateTimeWithTimeZone) -> Result<u64> {
        let prepared = agent_delegation_route::prepare_agent_delegation_route_expiry_batch(
            &self.connection,
            now,
        )
        .await?;
        self.run_serialized_write(|| {
            agent_delegation_route::expire_prepared_routes(&self.connection, &prepared, now)
        })
        .await
    }

    /// Persist the exact agent domain identity/execution graph for an agent
    /// Task occurrence before its runtime is started. The transaction owns
    /// immutable identity/lineage, root scope, independent liveness state and
    /// permit/queue placement together.
    pub async fn commit_agent_execution_graph(
        &self,
        input: AgentExecutionGraphCommitInput,
    ) -> Result<AgentExecutionGraphCommitResult> {
        let control_wakes = vec![GatewayControlWake::Resources, GatewayControlWake::Actions];
        let control_change = self.control_work_change("gateway.control", &control_wakes);
        let control_result = async {
            self.run_serialized_write(|| {
                let input = input.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin agent execution graph transaction")?;
                    match agent_transactions::commit_agent_execution_graph(&transaction, &input)
                        .await
                    {
                        Ok(result) => {
                            transaction
                                .commit()
                                .await
                                .context("failed to commit agent execution graph transaction")?;
                            Ok(result)
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            Err(error)
                        }
                    }
                }
            })
            .await
        }
        .await;
        if control_result.is_ok() {
            control_change.completed(true);
        }
        control_result
    }

    pub async fn promote_queued_agent_executions(
        &self,
        now: sea_orm::prelude::DateTimeWithTimeZone,
        limit: u64,
        idle_timeout_secs: i64,
        hard_timeout_secs: i64,
    ) -> Result<Vec<PromotedAgentExecution>> {
        let control_wakes = vec![GatewayControlWake::Resources, GatewayControlWake::Actions];
        let control_change = self.control_work_change("gateway.control", &control_wakes);
        let control_result = async {
            let change = pioneer_crud_tasks::work_change::TaskWorkChange::new(&self.connection);
            let task_result = async {
                self.run_serialized_write(|| {
                    let now = now.clone();
                    async move {
                        let transaction = self.connection.begin().await.context(
                            "failed to begin queued AgentExecution promotion transaction",
                        )?;
                        match agent_transactions::promote_queued_agent_executions(
                            &transaction,
                            now,
                            limit,
                            idle_timeout_secs,
                            hard_timeout_secs,
                        )
                        .await
                        {
                            Ok(promoted) => {
                                transaction.commit().await.context(
                                    "failed to commit queued AgentExecution promotion transaction",
                                )?;
                                Ok(promoted)
                            }
                            Err(error) => {
                                let _ = transaction.rollback().await;
                                Err(error)
                            }
                        }
                    }
                })
                .await
            }
            .await;
            if let Ok(value) = &task_result {
                change.completed(!value.is_empty());
            }
            task_result
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(!value.is_empty());
        }
        control_result
    }

    /// Atomically finalize one agent domain execution attempt, release only
    /// its permit/queue ownership and close the root scope when the graph is
    /// actually drained.
    pub async fn finalize_agent_execution(
        &self,
        execution_id: &str,
        terminal_status: &str,
        finished_at: sea_orm::prelude::DateTimeWithTimeZone,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Resources,
            GatewayControlWake::Actions,
            GatewayControlWake::Ledger,
        ];
        let control_change = self.control_work_change("gateway.control", &control_wakes);
        let control_result = async {
            let task_change = self.connection.work_signal("tasks.lifecycle").changing();
            let task_result =
                async {
                    self.run_serialized_write(|| {
                        let finished_at = finished_at.clone();
                        async move {
                            let transaction = self.connection.begin().await.context(
                                "failed to begin AgentExecution finalization transaction",
                            )?;
                            let finalized = pioneer_crud_tasks::terminal::finalize_agent_execution(
                                &transaction,
                                execution_id,
                                terminal_status,
                                finished_at,
                            )
                            .await?;
                            transaction.commit().await.context(
                                "failed to commit AgentExecution finalization transaction",
                            )?;
                            Ok(finalized)
                        }
                    })
                    .await
                }
                .await;
            if let Ok(changed) = &task_result {
                task_change.completed(*changed);
            }
            task_result
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(*value);
        }
        control_result
    }

    /// Outbox replay may release only the exact terminal Turn generation it
    /// observed. An explicit resume/recovery must not lose its new resources
    /// to a delayed delivery ACK or failure-cleanup callback.
    pub async fn finalize_terminal_agent_turn_owned(
        &self,
        expected: &TurnExecutionRecord,
        execution_id: &str,
        now: DateTimeWithTimeZone,
    ) -> Result<bool> {
        let control_wakes = vec![
            GatewayControlWake::Resources,
            GatewayControlWake::Actions,
            GatewayControlWake::Ledger,
        ];
        let control_change = self.control_work_change("gateway.control", &control_wakes);
        let control_result = async {
            let task_change = self.connection.work_signal("tasks.lifecycle").changing();
            let task_result = async {
                self.run_serialized_write(|| async {
                    let tx = self.connection.begin().await?;
                    let Some(current) = turn_execution::find(&tx, &expected.turn_id).await? else {
                        return Ok(false);
                    };
                    if current.owner_id != expected.owner_id
                        || current.owner_generation != expected.owner_generation
                        || current.thread_id != expected.thread_id
                        || current.workspace_id != expected.workspace_id
                        || !pioneer_crud_conversations::projection_receipts::has_accepted_terminal(
                            &tx,
                            &expected.turn_id,
                        )
                        .await?
                    {
                        return Ok(false);
                    }
                    let (turn_status, graph_status) = match current.status {
                        TurnExecutionStatus::Completed => (TurnStatus::Completed, "completed"),
                        TurnExecutionStatus::Failed => (TurnStatus::Failed, "failed"),
                        TurnExecutionStatus::Interrupted => (TurnStatus::Interrupted, "cancelled"),
                        TurnExecutionStatus::Blocked => (TurnStatus::Blocked, "blocked"),
                        _ => return Ok(false),
                    };
                    let saved = pioneer_crud_conversations::turn_reads::find_turn_by_thread_and_id(
                        &tx,
                        &current.thread_id,
                        &current.turn_id,
                    )
                    .await?
                    .context("terminal Agent Turn disappeared")?;
                    let response =
                        pioneer_crud_execution::execution_reads::load_agent_turn_response(
                            &tx,
                            &current.turn_id,
                        )
                        .await?
                        .context("terminal Agent Turn response disappeared")?;
                    if turn_status_from_db(&saved.status) != Some(turn_status)
                        || response.execution_id != execution_id
                    {
                        return Ok(false);
                    }
                    pioneer_crud_tasks::terminal::finalize_agent_execution(
                        &tx,
                        execution_id,
                        graph_status,
                        now,
                    )
                    .await?;
                    tx.commit().await?;
                    Ok(true)
                })
                .await
            }
            .await;
            if let Ok(changed) = &task_result {
                task_change.completed(*changed);
            }
            task_result
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(*value);
        }
        control_result
    }

    pub async fn cancel_agent_work_graph(
        &self,
        root_execution_id: &str,
        terminal_reason: &str,
        finished_at: sea_orm::prelude::DateTimeWithTimeZone,
    ) -> Result<Vec<AgentWorkGraphCancellationTarget>> {
        self.cancel_agent_work_graph_as(root_execution_id, terminal_reason, finished_at, None)
            .await
    }

    /// Fence the graph and retain authenticated Stop attribution together.
    /// `initiator` is (principal, originating thread, originating turn).
    /// System/agent cancellation leaves it absent; replay never rebinds runs.
    pub async fn cancel_agent_work_graph_as(
        &self,
        root_execution_id: &str,
        terminal_reason: &str,
        finished_at: sea_orm::prelude::DateTimeWithTimeZone,
        initiator: Option<(&str, &str, &str)>,
    ) -> Result<Vec<AgentWorkGraphCancellationTarget>> {
        let change = pioneer_crud_tasks::work_change::TaskWorkChange::new(&self.connection);
        let before = pioneer_crud_execution::execution_reads::find_agent_execution_status(
            &self.connection,
            root_execution_id,
        )
        .await?;
        let task_result = async {
            self.run_serialized_write(|| {
                let finished_at = finished_at.clone();
                async move {
                    let transaction = self
                        .connection
                        .begin()
                        .await
                        .context("failed to begin Agent work-graph cancellation transaction")?;
                    if let Some((actor, thread, turn)) = initiator {
                        pioneer_crud_tasks::task_cancellation_actor::record_graph(
                            &transaction,
                            actor,
                            thread,
                            turn,
                            root_execution_id,
                        )
                        .await?;
                    }
                    match agent_transactions::cancel_agent_work_graph(
                        &transaction,
                        root_execution_id,
                        terminal_reason,
                        finished_at,
                    )
                    .await
                    {
                        Ok(targets) => {
                            transaction.commit().await.context(
                                "failed to commit Agent work-graph cancellation transaction",
                            )?;
                            Ok(targets)
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            Err(error)
                        }
                    }
                }
            })
            .await
        }
        .await;
        if task_result.is_ok() {
            let after = pioneer_crud_execution::execution_reads::find_agent_execution_status(
                &self.connection,
                root_execution_id,
            )
            .await?;
            change.completed(before != after);
        }
        task_result
    }

    pub async fn cancel_agent_work_graph_and_collect_owners(
        &self,
        root_execution_id: &str,
        terminal_reason: &str,
        finished_at: sea_orm::prelude::DateTimeWithTimeZone,
    ) -> Result<Vec<AgentWorkGraphCancellationTarget>> {
        self.run_serialized_write(|| {
            let finished_at = finished_at.clone();
            async move {
                let transaction = self
                    .connection
                    .begin()
                    .await
                    .context("failed to begin Agent work-graph cancellation transaction")?;
                match agent_transactions::cancel_agent_work_graph_and_collect_owners(
                    &transaction,
                    root_execution_id,
                    terminal_reason,
                    finished_at,
                )
                .await
                {
                    Ok(targets) => {
                        transaction.commit().await.context(
                            "failed to commit Agent work-graph cancellation transaction",
                        )?;
                        Ok(targets)
                    }
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        Err(error)
                    }
                }
            }
        })
        .await
    }

    pub async fn load_execution_for_run(&self, run_id: &str) -> Result<Option<TaskRunExecution>> {
        self.task_execution_store()
            .load_execution_for_run(run_id)
            .await
    }

    pub async fn list_task_run_execution_observations(
        &self,
        run_ids: &[String],
    ) -> Result<Vec<TaskRunExecutionObservationRecord>> {
        self.task_execution_store()
            .list_task_run_execution_observations(run_ids)
            .await
    }

    pub async fn claim_execution(
        &self,
        execution_id: &str,
        worker_id: &str,
        lease_until: i64,
    ) -> Result<Option<TaskRunExecution>> {
        self.task_execution_store()
            .claim_execution(execution_id, worker_id, lease_until)
            .await
    }

    pub async fn claim_execution_at(
        &self,
        execution_id: &str,
        worker_id: &str,
        now: i64,
        lease_until: i64,
    ) -> Result<Option<TaskRunExecution>> {
        self.task_execution_store()
            .claim_execution_at(execution_id, worker_id, now, lease_until)
            .await
    }

    pub async fn mark_execution_running(
        &self,
        execution_id: &str,
        started_at: i64,
        lease_until: Option<i64>,
    ) -> Result<Option<TaskRunExecution>> {
        self.task_execution_store()
            .mark_execution_running(execution_id, started_at, lease_until)
            .await
    }

    pub async fn mark_execution_terminal(
        &self,
        execution_id: &str,
        status: TaskRunExecutionStatus,
        completed_at: i64,
        result: Option<&TaskResult>,
        error: Option<&TaskError>,
    ) -> Result<Option<TaskRunExecution>> {
        self.task_execution_store()
            .mark_execution_terminal(execution_id, status, completed_at, result, error)
            .await
    }
}
