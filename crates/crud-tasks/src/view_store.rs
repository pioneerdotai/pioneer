//! Scoped task aggregates, run history, result review, and delivery views.
use crate::delivery::{task_delivery_attempt_from_db_model, task_delivery_from_db_model};
use crate::task::{TaskRootAccessFilter, task_from_db_model};
use crate::task_agent_spec::task_agent_spec_from_db_model;
use crate::task_dependency::task_dependency_from_db_model;
use crate::task_result_candidate::task_result_candidate_from_db_model;
use crate::task_result_review_event::task_result_review_event_from_db_model;
use crate::task_run::task_run_from_db_model;
use crate::task_run_thread_binding::task_run_thread_binding_from_db_model;
use crate::task_run_turn::task_run_turn_from_db_model;
use crate::task_trigger::task_trigger_from_db_model;
use crate::values::unix_to_datetime;
use crate::work_change::TaskWorkChange;
use crate::write_lock::task_write_lock_from_db_model;
use crate::{
    delivery, task, task_actor_contract, task_agent_spec, task_dependency, task_result_candidate,
    task_result_review_event, task_run, task_run_execution, task_run_thread_binding, task_run_turn,
    task_trigger, write_lock,
};
use anyhow::{Context, Result, bail};
use pioneer_crud_conversations::thread_lineage::{self, task_thread_lineage_from_db_model};
use pioneer_crud_execution::{
    gateway_control::{GatewayControlWake, control_work_change},
    native_terminal_effect_outbox,
};
use pioneer_protocol::*;
use pioneer_sqlite::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, SqliteDatabase,
    retry_with_backoff,
};
use sea_orm::{EntityTrait, TransactionTrait};
use std::{collections::HashMap, future::Future, time::Duration};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskRunChildAnchor {
    pub child_thread_id: Option<String>,
    pub child_turn_id: Option<String>,
}

pub struct TaskViewStore<'a> {
    connection: &'a SqliteDatabase,
}

impl<'a> TaskViewStore<'a> {
    pub fn new(connection: &'a SqliteDatabase) -> Self {
        Self { connection }
    }

    async fn run_serialized_write<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry_with_backoff(
            operation,
            crate::retry::is_retryable,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    pub async fn get_task_run_turn(&self, id: &str) -> Result<Option<TaskRunTurn>> {
        crate::child_repository::TaskChildWorkRepository::new(self.connection)
            .get_task_run_turn(id)
            .await
    }

    pub async fn get_task_runs(&self, task_id: &str) -> Result<Vec<TaskRun>> {
        task_run::list_runs_by_task(self.connection, task_id)
            .await?
            .into_iter()
            .map(task_run_from_db_model)
            .collect()
    }

    pub async fn list_task_runs_by_status(
        &self,
        status: TaskRunStatus,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        task_run::list_runs_by_status(self.connection, status, limit)
            .await?
            .into_iter()
            .map(task_run_from_db_model)
            .collect()
    }

    pub async fn list_due_retry_task_runs(&self, now: i64, limit: u64) -> Result<Vec<TaskRun>> {
        task_run::list_due_retry_runs(self.connection, unix_to_datetime(now), limit)
            .await?
            .into_iter()
            .map(task_run_from_db_model)
            .collect()
    }

    pub async fn list_task_write_locks_by_run(&self, run_id: &str) -> Result<Vec<TaskWriteLock>> {
        write_lock::list_locks_by_run(self.connection, run_id)
            .await?
            .into_iter()
            .map(task_write_lock_from_db_model)
            .collect()
    }

    pub async fn list_active_task_write_locks(
        &self,
        workspace_id: &str,
        now: i64,
        limit: u64,
    ) -> Result<Vec<TaskWriteLock>> {
        write_lock::list_active_locks_for_workspace(
            self.connection,
            workspace_id,
            unix_to_datetime(now),
            limit,
        )
        .await?
        .into_iter()
        .map(task_write_lock_from_db_model)
        .collect()
    }

    pub async fn list_stale_task_write_locks(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<TaskWriteLock>> {
        write_lock::list_stale_locks(self.connection, unix_to_datetime(now), limit)
            .await?
            .into_iter()
            .map(task_write_lock_from_db_model)
            .collect()
    }

    pub async fn get_task(&self, task_id: &str) -> Result<Option<TaskGetResponse>> {
        let Some(task_model) = task::find_task_by_id(self.connection, task_id).await? else {
            return Ok(None);
        };

        let task = task_from_db_model(task_model)?;
        let triggers = task_trigger::list_triggers_by_task(self.connection, task_id)
            .await?
            .into_iter()
            .map(task_trigger_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        let runs = self.get_task_runs(task_id).await?;
        let agent_specs = task_agent_spec::list_agent_specs_by_task(self.connection, task_id)
            .await?
            .into_iter()
            .map(task_agent_spec_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        let dependencies = task_dependency::list_dependencies_for_task(self.connection, task_id)
            .await?
            .into_iter()
            .map(task_dependency_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        let write_locks = write_lock::list_locks_by_task(self.connection, task_id)
            .await?
            .into_iter()
            .map(task_write_lock_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        let mut task_run_thread_bindings = Vec::new();
        let mut task_run_turns = Vec::new();
        let mut result_candidates = Vec::new();
        let mut result_review_events = Vec::new();
        for run in &runs {
            task_run_thread_bindings.extend(self.list_task_run_thread_bindings(&run.id).await?);
            task_run_turns.extend(self.list_task_run_turns(&run.id).await?);
            result_candidates.extend(self.list_task_result_candidates(&run.id).await?);
            result_review_events
                .extend(self.list_task_result_review_events_for_run(&run.id).await?);
        }
        let mut thread_lineage = Vec::new();
        for binding in &task_run_thread_bindings {
            if thread_lineage
                .iter()
                .any(|lineage: &TaskThreadLineage| lineage.child_thread_id == binding.thread_id)
            {
                continue;
            }
            if let Some(lineage) =
                thread_lineage::find_lineage_by_child_thread(self.connection, &binding.thread_id)
                    .await?
            {
                thread_lineage.push(task_thread_lineage_from_db_model(lineage));
            }
        }

        Ok(Some(TaskGetResponse {
            task,
            triggers,
            runs,
            agent_specs,
            dependencies,
            write_locks,
            thread_lineage,
            task_run_thread_bindings,
            task_run_turns,
            result_candidates,
            result_review_events,
        }))
    }

    pub async fn upsert_task_actor_contract(
        &self,
        contract: &pioneer_protocol::TaskActorContract,
        now: i64,
    ) -> Result<()> {
        let change = TaskWorkChange::for_tasks(self.connection, [contract.task_id.clone()]);
        let before = self.get_task_actor_contract(&contract.task_id).await?;
        let result = async {
            task_actor_contract::upsert_task_actor_contract(self.connection, contract, now).await
        }
        .await;
        if result.is_ok() {
            let after = self.get_task_actor_contract(&contract.task_id).await?;
            change.completed(before != after);
        }
        result
    }

    pub async fn get_task_actor_contract(
        &self,
        task_id: &str,
    ) -> Result<Option<pioneer_protocol::TaskActorContract>> {
        task_actor_contract::find_task_actor_contract(self.connection, task_id).await
    }

    pub async fn list_tasks_created_by_turn(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Vec<Task>> {
        task::list_tasks_by_creator_turn(self.connection, workspace_id, thread_id, turn_id)
            .await?
            .into_iter()
            .map(task_from_db_model)
            .collect()
    }

    pub async fn upsert_task_run_thread_binding(
        &self,
        binding: TaskRunThreadBinding,
    ) -> Result<TaskRunThreadBinding> {
        let change = TaskWorkChange::for_tasks(self.connection, [binding.task_id.clone()]);
        let before = self.get_task_run_thread_binding(&binding.id).await?;
        let result = async {
            self.run_serialized_write(|| async {
                task_run_thread_binding::upsert_binding(
                    self.connection,
                    task_run_thread_binding::NewTaskRunThreadBinding {
                        id: binding.id.clone(),
                        task_id: binding.task_id.clone(),
                        run_id: binding.run_id.clone(),
                        execution_id: binding.execution_id.clone(),
                        thread_id: binding.thread_id.clone(),
                        binding_kind: binding.binding_kind,
                        created_at: binding.created_at,
                    },
                )
                .await?;
                let row = task_run_thread_binding::find_binding_by_id(self.connection, &binding.id)
                    .await?
                    .context("task run thread binding missing after upsert")?;
                task_run_thread_binding_from_db_model(row)
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(before.as_ref() != Some(value));
        }
        result
    }

    pub async fn get_task_run_thread_binding(
        &self,
        id: &str,
    ) -> Result<Option<TaskRunThreadBinding>> {
        task_run_thread_binding::find_binding_by_id(self.connection, id)
            .await?
            .map(task_run_thread_binding_from_db_model)
            .transpose()
    }

    pub async fn get_task_run_primary_thread_binding(
        &self,
        run_id: &str,
    ) -> Result<Option<TaskRunThreadBinding>> {
        task_run_thread_binding::find_binding_by_run_and_kind(
            self.connection,
            run_id,
            TaskRunThreadBindingKind::PrimaryExecutor,
        )
        .await?
        .map(task_run_thread_binding_from_db_model)
        .transpose()
    }

    pub async fn get_task_run_thread_binding_by_thread(
        &self,
        thread_id: &str,
    ) -> Result<Option<TaskRunThreadBinding>> {
        task_run_thread_binding::find_binding_by_thread(self.connection, thread_id)
            .await?
            .map(task_run_thread_binding_from_db_model)
            .transpose()
    }

    pub async fn list_task_run_thread_bindings(
        &self,
        run_id: &str,
    ) -> Result<Vec<TaskRunThreadBinding>> {
        task_run_thread_binding::list_bindings_by_run(self.connection, run_id)
            .await?
            .into_iter()
            .map(task_run_thread_binding_from_db_model)
            .collect()
    }

    pub async fn get_task_run_turn_by_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<TaskRunTurn>> {
        task_run_turn::find_turn_by_thread_and_turn(self.connection, thread_id, turn_id)
            .await?
            .map(task_run_turn_from_db_model)
            .transpose()
    }

    pub async fn list_task_run_turns(&self, run_id: &str) -> Result<Vec<TaskRunTurn>> {
        task_run_turn::list_turns_by_run(self.connection, run_id)
            .await?
            .into_iter()
            .map(task_run_turn_from_db_model)
            .collect()
    }

    pub async fn get_latest_task_run_turn(&self, run_id: &str) -> Result<Option<TaskRunTurn>> {
        task_run_turn::find_latest_turn_by_run(self.connection, run_id)
            .await?
            .map(task_run_turn_from_db_model)
            .transpose()
    }

    pub async fn update_task_run_turn_status(
        &self,
        id: &str,
        status: TaskRunTurnStatus,
        completed_at: Option<i64>,
    ) -> Result<Option<TaskRunTurn>> {
        let change = TaskWorkChange::for_source(self.connection, "v", id);
        let before = self.get_task_run_turn(id).await?;
        let result = async {
            self.run_serialized_write(|| async {
                let transaction = self.connection.begin().await?;
                let row = task_run_turn::update_turn_status(&transaction, id, status, completed_at)
                    .await?;
                transaction.commit().await?;
                row.map(task_run_turn_from_db_model).transpose()
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(before.as_ref() != value.as_ref());
        }
        result
    }

    pub async fn upsert_task_delivery_authority(
        &self,
        delivery_id: &str,
        task_id: &str,
        run_id: &str,
        author_json: &str,
        reviewer_json: Option<&str>,
        destination_route_id: Option<&str>,
        route_receipt_json: Option<&str>,
        disclosure_generation: u64,
        idempotency_key: &str,
        status: &str,
        now: i64,
    ) -> Result<()> {
        let change = TaskWorkChange::for_tasks(self.connection, [task_id.to_owned()]);
        let mut before =
            pioneer_entity::task_delivery_authority::Entity::find_by_id(delivery_id.to_owned())
                .one(self.connection)
                .await?;
        let result = async {
            task_actor_contract::upsert_task_delivery_authority(
                self.connection,
                delivery_id,
                task_id,
                run_id,
                author_json,
                reviewer_json,
                destination_route_id,
                route_receipt_json,
                disclosure_generation,
                idempotency_key,
                status,
                now,
            )
            .await
        }
        .await;
        if result.is_ok() {
            let mut after =
                pioneer_entity::task_delivery_authority::Entity::find_by_id(delivery_id.to_owned())
                    .one(self.connection)
                    .await?;
            // Updated-at alone is not a dependency change.
            if let (Some(old), Some(new)) = (&mut before, &mut after) {
                new.updated_at = old.updated_at;
            }
            change.completed(before != after);
        }
        result
    }

    pub async fn upsert_task_result_candidate(
        &self,
        candidate: TaskResultCandidate,
    ) -> Result<TaskResultCandidate> {
        let control_wakes = vec![GatewayControlWake::Actions];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            let change = TaskWorkChange::for_tasks(self.connection, [candidate.task_id.clone()]);
            let before = self.get_task_result_candidate(&candidate.id).await?;
            let result = async {
                let prepared_candidate =
                    task_result_candidate::prepare_protocol_candidate(&candidate)?;
                let persisted = self
                    .run_serialized_write(|| {
                        let prepared_candidate = prepared_candidate.clone();
                        async {
                            let candidate_status =
                                crate::status::task_result_candidate_status_to_db(candidate.status);
                            let gate_resolution =
                        native_terminal_effect_outbox::prepare_gate_resolution_for_candidate(
                            self.connection,
                            Some(native_terminal_effect_outbox::CandidateGateMetadata {
                                id: candidate.id.clone(),
                                status: candidate_status.to_owned(),
                                updated_at: unix_to_datetime(candidate.updated_at),
                            }),
                            candidate.thread_id.as_str(),
                            candidate.turn_id.as_str(),
                            unix_to_datetime(candidate.updated_at),
                            None,
                        )
                        .await?;
                            let transaction = self.connection.begin().await.context(
                                "failed to begin task result candidate upsert transaction",
                            )?;
                            let result = async {
                                task_result_candidate::upsert_candidate(
                                    &transaction,
                                    prepared_candidate,
                                )
                                .await?;
                                native_terminal_effect_outbox::apply_prepared_gate_resolution(
                                    &transaction,
                                    gate_resolution,
                                )
                                .await?;
                                let row = task_result_candidate::find_candidate_by_id(
                                    &transaction,
                                    &candidate.id,
                                )
                                .await?
                                .context("task result candidate missing after upsert")?;
                                Ok(row)
                            }
                            .await;
                            match result {
                                Ok(candidate) => {
                                    transaction.commit().await.context(
                                        "failed to commit task result candidate upsert transaction",
                                    )?;
                                    Ok(candidate)
                                }
                                Err(error) => {
                                    let _ = transaction.rollback().await;
                                    Err(error)
                                }
                            }
                        }
                    })
                    .await?;
                task_result_candidate_from_db_model(persisted)
            }
            .await;
            if let Ok(value) = &result {
                change.completed(before.as_ref() != Some(value));
            }
            result
        }
        .await;
        if control_result.is_ok() {
            control_change.completed(true);
        }
        control_result
    }

    pub async fn update_task_result_candidate_resolution(
        &self,
        id: &str,
        status: TaskResultCandidateStatus,
        final_review_event_id: Option<&str>,
        resolved_at: Option<i64>,
        updated_at: i64,
    ) -> Result<Option<TaskResultCandidate>> {
        let control_wakes = vec![GatewayControlWake::Actions];
        let control_change =
            control_work_change(self.connection, "gateway.control", &control_wakes);
        let control_result = async {
            let persisted = self
                .run_serialized_write(|| async {
                    let Some(current) =
                        task_result_candidate::find_candidate_by_id(self.connection, id).await?
                    else {
                        return Ok(None);
                    };
                    let desired_status =
                        crate::status::task_result_candidate_status_to_db(status).to_owned();
                    let gate_resolution =
                        native_terminal_effect_outbox::prepare_gate_resolution_for_candidate(
                            self.connection,
                            Some(native_terminal_effect_outbox::CandidateGateMetadata {
                                id: current.id.clone(),
                                status: desired_status.clone(),
                                updated_at: if current.status == desired_status {
                                    current.updated_at
                                } else {
                                    unix_to_datetime(updated_at)
                                },
                            }),
                            current.thread_id.as_str(),
                            current.turn_id.as_str(),
                            unix_to_datetime(updated_at),
                            None,
                        )
                        .await?;
                    let transaction =
                        self.connection.begin().await.context(
                            "failed to begin task result candidate resolution transaction",
                        )?;
                    let result = async {
                        let candidate = task_result_candidate::update_candidate_resolution(
                            &transaction,
                            id,
                            status,
                            final_review_event_id,
                            resolved_at,
                            updated_at,
                        )
                        .await?;
                        if let Some(candidate) = candidate.as_ref() {
                            // The gate locator was read before writer admission.
                            if candidate.thread_id != current.thread_id
                                || candidate.turn_id != current.turn_id
                            {
                                bail!("candidate gate locator changed before resolution");
                            }
                            native_terminal_effect_outbox::apply_prepared_gate_resolution(
                                &transaction,
                                gate_resolution,
                            )
                            .await?;
                        }
                        Ok(candidate)
                    }
                    .await;
                    match result {
                        Ok(candidate) => {
                            transaction.commit().await.context(
                                "failed to commit task result candidate resolution transaction",
                            )?;
                            Ok(candidate)
                        }
                        Err(error) => {
                            let _ = transaction.rollback().await;
                            Err(error)
                        }
                    }
                })
                .await?;
            persisted
                .map(task_result_candidate_from_db_model)
                .transpose()
        }
        .await;
        if let Ok(value) = &control_result {
            control_change.completed(value.is_some());
        }
        control_result
    }

    pub async fn get_task_result_candidate(&self, id: &str) -> Result<Option<TaskResultCandidate>> {
        task_result_candidate::find_candidate_by_id(self.connection, id)
            .await?
            .map(task_result_candidate_from_db_model)
            .transpose()
    }

    pub async fn get_task_result_candidate_by_turn(
        &self,
        task_run_turn_id: &str,
    ) -> Result<Option<TaskResultCandidate>> {
        task_result_candidate::find_candidate_by_turn(self.connection, task_run_turn_id)
            .await?
            .map(task_result_candidate_from_db_model)
            .transpose()
    }

    pub async fn list_task_result_candidates(
        &self,
        run_id: &str,
    ) -> Result<Vec<TaskResultCandidate>> {
        task_result_candidate::list_candidates_by_run(self.connection, run_id)
            .await?
            .into_iter()
            .map(task_result_candidate_from_db_model)
            .collect()
    }

    pub async fn get_accepted_task_result_candidate(
        &self,
        run_id: &str,
    ) -> Result<Option<TaskResultCandidate>> {
        task_result_candidate::find_candidate_by_run_and_status(
            self.connection,
            run_id,
            TaskResultCandidateStatus::Accepted,
        )
        .await?
        .map(task_result_candidate_from_db_model)
        .transpose()
    }

    pub async fn get_pending_task_result_candidate(
        &self,
        run_id: &str,
    ) -> Result<Option<TaskResultCandidate>> {
        task_result_candidate::find_candidate_by_run_and_status(
            self.connection,
            run_id,
            TaskResultCandidateStatus::PendingReview,
        )
        .await?
        .map(task_result_candidate_from_db_model)
        .transpose()
    }

    pub async fn upsert_task_result_review_event(
        &self,
        review_event: TaskResultReviewEvent,
    ) -> Result<TaskResultReviewEvent> {
        let change = TaskWorkChange::for_tasks(self.connection, [review_event.task_id.clone()]);
        let before = self.get_task_result_review_event(&review_event.id).await?;
        let result = async {
            let prepared_review =
                task_result_review_event::prepare_protocol_review_event(&review_event)?;
            self.run_serialized_write(|| {
                let prepared_review = prepared_review.clone();
                async {
                    task_result_review_event::upsert_review_event(self.connection, prepared_review)
                        .await?;
                    let row = task_result_review_event::find_review_event_by_id(
                        self.connection,
                        &review_event.id,
                    )
                    .await?
                    .context("task result review event missing after upsert")?;
                    task_result_review_event_from_db_model(row)
                }
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(before.as_ref() != Some(value));
        }
        result
    }

    pub async fn get_task_result_review_event(
        &self,
        id: &str,
    ) -> Result<Option<TaskResultReviewEvent>> {
        task_result_review_event::find_review_event_by_id(self.connection, id)
            .await?
            .map(task_result_review_event_from_db_model)
            .transpose()
    }

    pub async fn list_task_result_review_events(
        &self,
        candidate_id: &str,
    ) -> Result<Vec<TaskResultReviewEvent>> {
        task_result_review_event::list_review_events_by_candidate(self.connection, candidate_id)
            .await?
            .into_iter()
            .map(task_result_review_event_from_db_model)
            .collect()
    }

    pub async fn list_task_result_review_events_for_run(
        &self,
        run_id: &str,
    ) -> Result<Vec<TaskResultReviewEvent>> {
        task_result_review_event::list_review_events_by_run(self.connection, run_id)
            .await?
            .into_iter()
            .map(task_result_review_event_from_db_model)
            .collect()
    }

    pub async fn get_task_run_child_anchor(&self, run_id: &str) -> Result<TaskRunChildAnchor> {
        let primary_binding = self.get_task_run_primary_thread_binding(run_id).await?;
        let accepted_candidate = self.get_accepted_task_result_candidate(run_id).await?;
        let accepted_turn = match accepted_candidate.as_ref() {
            Some(candidate) => {
                self.get_task_run_turn(candidate.task_run_turn_id.as_str())
                    .await?
            }
            None => None,
        };
        let latest_turn = self.get_latest_task_run_turn(run_id).await?;

        let target_anchor = TaskRunChildAnchor {
            child_thread_id: primary_binding
                .as_ref()
                .map(|binding| binding.thread_id.clone())
                .or_else(|| accepted_turn.as_ref().map(|turn| turn.thread_id.clone()))
                .or_else(|| latest_turn.as_ref().map(|turn| turn.thread_id.clone())),
            child_turn_id: accepted_turn
                .as_ref()
                .map(|turn| turn.turn_id.clone())
                .or_else(|| latest_turn.as_ref().map(|turn| turn.turn_id.clone())),
        };
        Ok(target_anchor)
    }

    pub async fn count_task_run_executions_for_run(&self, run_id: &str) -> Result<u64> {
        task_run_execution::count_executions_by_run(self.connection, run_id).await
    }

    pub async fn get_task_deliveries_by_ids(
        &self,
        workspace: &str,
        thread: &str,
        ids: &[String],
    ) -> Result<HashMap<String, TaskDelivery>> {
        delivery::find_deliveries_by_ids(self.connection, workspace, thread, ids)
            .await?
            .into_iter()
            .map(|row| {
                let value = task_delivery_from_db_model(row)?;
                Ok((value.id.clone(), value))
            })
            .collect()
    }

    pub async fn get_task_run_turns_by_sources(
        &self,
        workspace: &str,
        sources: &[(String, String)],
    ) -> Result<HashMap<(String, String), TaskRunTurn>> {
        task_run_turn::find_turns_by_sources(self.connection, workspace, sources)
            .await?
            .into_iter()
            .map(|row| {
                let value = task_run_turn_from_db_model(row)?;
                Ok(((value.thread_id.clone(), value.turn_id.clone()), value))
            })
            .collect()
    }

    pub async fn get_task_result_candidates_by_turns(
        &self,
        ids: &[String],
    ) -> Result<HashMap<String, TaskResultCandidate>> {
        task_result_candidate::find_candidates_by_turns(self.connection, ids)
            .await?
            .into_iter()
            .map(|row| {
                let value = task_result_candidate_from_db_model(row)?;
                Ok((value.task_run_turn_id.clone(), value))
            })
            .collect()
    }

    pub async fn get_task_delivery(&self, delivery_id: &str) -> Result<Option<TaskDelivery>> {
        delivery::find_delivery_by_id(self.connection, delivery_id)
            .await?
            .map(task_delivery_from_db_model)
            .transpose()
    }

    pub async fn list_due_task_deliveries(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<TaskDelivery>> {
        delivery::list_due_deliveries(self.connection, unix_to_datetime(now), limit)
            .await?
            .into_iter()
            .map(task_delivery_from_db_model)
            .collect()
    }

    pub async fn list_task_deliveries(
        &self,
        params: TaskDeliveriesParams,
    ) -> Result<TaskDeliveriesResponse> {
        self.list_task_deliveries_scoped(params, None).await
    }

    pub async fn list_task_deliveries_scoped(
        &self,
        params: TaskDeliveriesParams,
        access: Option<&TaskRootAccessFilter>,
    ) -> Result<TaskDeliveriesResponse> {
        let deliveries = delivery::list_deliveries_scoped(
            self.connection,
            &params,
            access.map(task::accessible_task_ids),
        )
        .await?
        .into_iter()
        .map(task_delivery_from_db_model)
        .collect::<Result<Vec<_>>>()?;
        let delivery_ids = deliveries
            .iter()
            .map(|delivery| delivery.id.clone())
            .collect::<Vec<_>>();
        let attempts = delivery::list_attempts_for_deliveries(self.connection, &delivery_ids)
            .await?
            .into_iter()
            .map(task_delivery_attempt_from_db_model)
            .collect::<Result<Vec<_>>>()?;
        Ok(TaskDeliveriesResponse {
            deliveries,
            attempts,
        })
    }
}

#[cfg(test)]
#[path = "view_store_tests.rs"]
mod tests;
