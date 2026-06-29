use super::agent_runtime::TurnFailureRecoveryKind;
use super::*;
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use pioneer_agent::{AgentTurnHookRuntimeContext, ExecutionCheckpointContext};
use pioneer_promt::{TaskRevisionPromptInput, TaskRunPromptCompiler, TaskRunPromptInput};
use pioneer_protocol::{
    ExecutionCheckpointPayload, ItemCompletedNotification, ItemStartedNotification,
    PermissionBehavior, SandboxMode, Task, TaskAgentContext, TaskAgentContextMode, TaskAgentInput,
    TaskAgentResultContract, TaskAgentResultFormat, TaskAgentReviewPolicy, TaskAgentSpec,
    TaskAgentToolPolicy, TaskAgentWriteMode, TaskAttachmentMode, TaskError, TaskErrorClass,
    TaskExecutorKind, TaskGetResponse, TaskResult, TaskResultCandidate, TaskResultCandidateStatus,
    TaskResultReviewDecision, TaskResultReviewEvent, TaskResultReviewEventKind,
    TaskResultReviewerKind, TaskResultReviewerSpec, TaskReviseResponse, TaskRun, TaskRunExecution,
    TaskRunExecutionStatus, TaskRunStatus, TaskRunThreadBinding, TaskRunThreadBindingKind,
    TaskRunTurn, TaskRunTurnKind, TaskRunTurnStatus, TaskThreadLineage, TaskTrigger,
    TaskTriggerKind, TaskValue, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
    ThreadStatus, ToolPermissionPolicySnapshot, Turn, TurnBlockedNotification,
    TurnCompletedNotification, TurnFailedNotification, TurnKind, TurnOrigin,
    TurnPermissionProfileSnapshot, TurnPermissionProfileSource, TurnStartParams,
    TurnStartedNotification, TurnStatus, UserInput,
};
use pioneer_tasks::{
    CreateTaskResultReviewerContextParams, RecordTaskResultReviewEventParams,
    TASK_EXECUTION_LEASE_SECONDS, TaskExecutionContext, TaskExecutionHandle, TaskExecutor,
    TaskExecutorRecoveryOutcome, TaskExecutorStartOutcome, TaskResultReviewActor,
    WriteLockDecision, stable_review_thread_id, stable_review_turn_id,
    task_result_reviewer_spec_key,
};
use std::collections::BTreeMap;
use std::sync::{RwLock as StdRwLock, Weak};
use tokio::time::{Duration, sleep};

const TASK_EXECUTION_HEARTBEAT_SECONDS: u64 = 30;

#[derive(Default)]
pub(crate) struct TaskAgentExecutor {
    processor: StdRwLock<Option<Weak<MessageProcessor>>>,
}

impl TaskAgentExecutor {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn bind(&self, processor: Weak<MessageProcessor>) {
        if let Ok(mut guard) = self.processor.write() {
            *guard = Some(processor);
        }
    }

    fn processor(&self) -> Result<Arc<MessageProcessor>> {
        let weak = self
            .processor
            .read()
            .map_err(|_| anyhow!("task agent executor lock is poisoned"))?
            .clone()
            .ok_or_else(|| anyhow!("task agent executor is not bound"))?;
        weak.upgrade()
            .ok_or_else(|| anyhow!("message processor is no longer available"))
    }

    async fn start_or_recover_run(
        &self,
        context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        let processor = self.processor()?;
        let Some(task_response) = processor.crud_store.get_task(run.task_id.as_str()).await? else {
            bail!("task `{}` not found", run.task_id);
        };
        if task_response.task.status.is_terminal() || run.status.is_terminal() {
            return Ok(TaskExecutorStartOutcome::Queued);
        }
        let agent_spec = select_agent_spec(&task_response, run.id.as_str())
            .ok_or_else(|| anyhow!("agent task `{}` has no agent spec", run.task_id))?;
        if run.status == TaskRunStatus::WaitingReview {
            return self
                .recover_waiting_review_run(&processor, &task_response, &run, &agent_spec)
                .await;
        }
        let Some(execution) = self
            .load_or_reserve_execution(&processor, &context, &run)
            .await?
        else {
            return Ok(TaskExecutorStartOutcome::Queued);
        };

        if let Some(child_runtime) = load_child_runtime_for_run(&processor, run.id.as_str()).await?
        {
            return self
                .recover_existing_child_turn(
                    &processor,
                    &task_response,
                    &run,
                    &agent_spec,
                    &execution,
                    child_runtime,
                    handle,
                )
                .await;
        }

        let parent = resolve_parent_context(&processor, &task_response.task).await?;
        match self
            .acquire_write_locks(&processor, &task_response.task, &run, handle.clone())
            .await?
        {
            TaskExecutorStartOutcome::Started => {}
            outcome => return Ok(outcome),
        }
        let occurrence_permission_profile =
            effective_task_child_permission_profile(&agent_spec, None)?;
        let parent = ensure_task_run_occurrence_context(
            &processor,
            &task_response,
            &run,
            parent,
            &occurrence_permission_profile,
        )
        .await?;
        self.start_new_child_turn(
            &processor,
            &context,
            &task_response,
            &run,
            &agent_spec,
            &parent,
            execution,
            handle,
        )
        .await
    }

    async fn load_or_reserve_execution(
        &self,
        processor: &Arc<MessageProcessor>,
        context: &TaskExecutionContext,
        run: &TaskRun,
    ) -> Result<Option<TaskRunExecution>> {
        let now = now_timestamp_secs();
        let reserved = processor
            .crud_store
            .reserve_execution_for_run(run.id.as_str(), TaskExecutorKind::Agent, now)
            .await
            .context("failed to reserve task run execution")?;
        if let Some(context_execution_id) = context.execution_id.as_deref()
            && context_execution_id != reserved.id
        {
            bail!(
                "task run `{}` context execution `{}` does not match reserved execution `{}`",
                run.id,
                context_execution_id,
                reserved.id
            );
        }
        let lease_until = now.saturating_add(TASK_EXECUTION_LEASE_SECONDS);
        let claimed = processor
            .crud_store
            .claim_execution_at(
                reserved.id.as_str(),
                context.worker_id.as_str(),
                now,
                lease_until,
            )
            .await
            .context("failed to claim task run execution")?;
        Ok(claimed)
    }

    async fn recover_waiting_review_run(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
    ) -> Result<TaskExecutorStartOutcome> {
        let Some(review_policy) = agent_spec
            .review_policy
            .as_ref()
            .filter(|policy| policy.is_enabled())
        else {
            return Ok(TaskExecutorStartOutcome::Queued);
        };
        let mut candidates = processor
            .crud_store
            .list_task_result_candidates(run.id.as_str())
            .await?;
        candidates.retain(|candidate| {
            matches!(
                candidate.status,
                TaskResultCandidateStatus::PendingReview
                    | TaskResultCandidateStatus::ExtractionFailed
            )
        });
        candidates.sort_by(|left, right| {
            right
                .round
                .cmp(&left.round)
                .then_with(|| right.created_at.cmp(&left.created_at))
        });
        if let Some(candidate) = candidates.first() {
            self.start_reviewer_turns_for_candidate(
                processor,
                task_response,
                agent_spec,
                review_policy,
                candidate,
            )
            .await?;
        }
        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn start_new_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        context: &TaskExecutionContext,
        task_response: &TaskGetResponse,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
        parent: &TaskParentRuntimeContext,
        execution: TaskRunExecution,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        let task = &task_response.task;
        let now = now_timestamp_secs();
        let task_run_turn = initial_task_run_turn_from_execution(task, run, &execution, now);
        let binding =
            task_run_primary_binding_from_turn(task, run, &execution, &task_run_turn, now);
        let child_runtime = TaskRunChildRuntime {
            lineage: lineage_from_task_run_turn(task, run, agent_spec, parent, &task_run_turn, now),
            task_run_turn,
        };
        let child_thread_id = child_runtime.task_run_turn.thread_id.clone();
        let child_turn_id = child_runtime.task_run_turn.turn_id.clone();
        let effective_model = effective_agent_model(agent_spec)?;
        let child_permission_profile = effective_task_child_permission_profile(agent_spec, None)?;
        let thread_params = pioneer_protocol::ThreadStartParams {
            thread_id: child_thread_id.clone(),
            workspace_id: context.workspace_id.clone(),
            name: thread_name_from_task(task),
            model: Some(effective_model.model.clone()),
            model_provider: Some(effective_model.model_provider.clone()),
            sandbox: Some(SandboxMode::FullAccess),
            mode: Some(ThreadMode::Agent),
            origin_kind: Some(ThreadOriginKind::TaskRun),
            sidebar_visibility: Some(ThreadSidebarVisibility::Hidden),
            agent_nickname: agent_spec.agent_nickname.clone(),
            agent_role: agent_spec.agent_role.clone(),
        };
        let thread_outcome = processor
            .thread_manager
            .system_thread_start_seeded(context.workspace_id.clone(), thread_params, None, None)
            .await
            .context("failed to create hidden task thread")?;

        let prompt = materialize_child_task_prompt(
            processor,
            task_response,
            run,
            agent_spec,
            parent,
            None,
            &child_permission_profile,
        )
        .await?;
        let child_input = materialize_child_task_input(prompt, agent_spec);
        let turn_outcome = processor
            .thread_manager
            .system_turn_start_with_permission_profile(
                TurnStartParams {
                    thread_id: child_thread_id.clone(),
                    turn_id: child_turn_id.clone(),
                    input: child_input,
                    capabilities: Vec::new(),
                    model: Some(effective_model.model),
                    model_provider: Some(effective_model.model_provider),
                    sandbox_policy: None,
                    mode: Some(ThreadMode::Agent),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
                },
                child_permission_profile,
            )
            .await
            .context("failed to create hidden task turn")?;

        if let Err(error) = processor
            .validate_artifact_user_inputs(
                context.workspace_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).context("failed to validate hidden task artifact input");
        }

        let turn_permission_profile = match processor
            .materialized_turn_permission_profile(&turn_outcome.materialization.turn)
        {
            Ok(permission_profile) => permission_profile,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                return Err(error).context("failed to resolve hidden task permission profile");
            }
        };
        let profile_selected_audit = processor.turn_profile_selected_audit_event_for_turn(
            context.workspace_id.as_str(),
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            turn_permission_profile.clone(),
        );
        if let Err(error) = message_future(
            processor
                .crud_store
                .materialize_turn_start_with_permission_audit(
                    &turn_outcome.materialization.thread,
                    turn_outcome.materialization.sandbox_mode,
                    &turn_outcome.materialization.turn,
                    &turn_outcome.materialization.input,
                    profile_selected_audit,
                ),
        )
        .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).context("failed to persist hidden task turn");
        }
        handle
            .link_child_thread_with_runtime(
                child_runtime.lineage.clone(),
                binding,
                child_runtime.task_run_turn.clone(),
                now,
            )
            .await?;

        processor.ensure_hook_runtime_with_run_store().await;
        processor
            .agent_manager
            .ensure_thread(child_thread_id.as_str(), context.workspace_id.as_str())
            .await
            .map_err(|error| anyhow!("failed to prepare child agent runtime: {error}"))?;
        processor
            .ensure_agent_listener_task(child_thread_id.as_str())
            .await;

        let started_at = now_timestamp_secs();
        handle.mark_started(started_at).await?;
        processor
            .crud_store
            .mark_execution_running(
                execution.id.as_str(),
                started_at,
                Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
            )
            .await
            .context("failed to mark task run execution running")?;
        let workspace_skill_policies =
            load_workspace_skill_policies(processor, task.workspace_id.as_str()).await;
        let resolved_artifacts = processor
            .resolve_provider_artifact_inputs(
                task.workspace_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
            .context("failed to resolve hidden task artifact input for provider")?;
        let runtime_environment = processor
            .create_artifact_output_environment(
                task.workspace_id.as_str(),
                child_thread_id.as_str(),
                child_turn_id.as_str(),
            )
            .await
            .context("failed to prepare hidden task artifact output directory")?
            .into_iter()
            .collect();
        let hook_runtime_context = AgentTurnHookRuntimeContext::task(task.id.clone());
        if let Err(error) = processor
            .persist_turn_runtime_snapshot(
                child_thread_id.as_str(),
                task.workspace_id.as_str(),
                child_turn_id.as_str(),
                ThreadMode::Agent,
                &hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                None,
                &workspace_skill_policies,
                turn_outcome.materialization.input.as_slice(),
                turn_outcome.materialization.capabilities.as_slice(),
                resolved_artifacts.as_slice(),
                &runtime_environment,
                &[],
            )
            .await
        {
            processor
                .report_turn_failure(
                    child_thread_id,
                    child_turn_id,
                    TurnFailureRecoveryKind::TaskDispatch,
                    format!("failed to persist child task turn runtime snapshot: {error:#}"),
                )
                .await;
            return Ok(TaskExecutorStartOutcome::Started);
        }
        let runtime_permission_profile = turn_permission_profile;
        if let Err(error) = processor
            .agent_manager
            .start_turn_with_hook_context_and_permission_profile(
                child_thread_id.as_str(),
                child_turn_id.as_str(),
                ThreadMode::Agent,
                hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                workspace_skill_policies,
                turn_outcome.materialization.input,
                turn_outcome.materialization.capabilities,
                resolved_artifacts,
                runtime_environment,
                Vec::new(),
                runtime_permission_profile,
            )
            .await
        {
            processor
                .report_turn_failure(
                    child_thread_id,
                    child_turn_id,
                    TurnFailureRecoveryKind::TaskDispatch,
                    format!("failed to dispatch child task turn: {error}"),
                )
                .await;
            return Ok(TaskExecutorStartOutcome::Started);
        }
        spawn_execution_heartbeat(
            processor,
            execution.id,
            child_thread_id,
            child_turn_id,
            run.id.clone(),
        );

        Ok(TaskExecutorStartOutcome::Started)
    }

    pub(crate) async fn dispatch_revision_turn(
        &self,
        response: TaskReviseResponse,
    ) -> Result<TaskReviseResponse> {
        let processor = self.processor()?;
        let task_response =
            message_future(processor.crud_store.get_task(response.task.id.as_str()))
                .await?
                .ok_or_else(|| anyhow!("task `{}` not found", response.task.id))?;
        let run = task_response
            .runs
            .iter()
            .find(|run| run.id == response.run.id)
            .cloned()
            .ok_or_else(|| anyhow!("task run `{}` not found", response.run.id))?;
        let agent_spec = select_agent_spec(&task_response, run.id.as_str())
            .ok_or_else(|| anyhow!("agent task `{}` has no agent spec", task_response.task.id))?;
        let execution =
            match message_future(processor.crud_store.load_execution_for_run(run.id.as_str()))
                .await?
            {
                Some(execution) => execution,
                None => {
                    message_future(processor.crud_store.reserve_execution_for_run(
                        run.id.as_str(),
                        TaskExecutorKind::Agent,
                        now_timestamp_secs(),
                    ))
                    .await?
                }
            };
        let child_runtime = message_future(load_child_runtime_from_task_run_turn(
            &processor,
            response.task_run_turn.clone(),
        ))
        .await?;
        let handle = TaskExecutionHandle::new(
            processor.crud_store.clone(),
            processor.task_runtime.event_bus(),
            run.task_id.clone(),
            run.id.clone(),
        );
        message_future(self.dispatch_existing_revision_turn(
            &processor,
            &task_response,
            &run,
            &agent_spec,
            &execution,
            child_runtime,
            handle,
        ))
        .await?;
        message_future(task_revise_response_from_store(&processor, response)).await
    }

    async fn dispatch_existing_revision_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
        execution: &TaskRunExecution,
        child_runtime: TaskRunChildRuntime,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        let task = &task_response.task;
        let child_thread_id = child_runtime.task_run_turn.thread_id.clone();
        let child_turn_id = child_runtime.task_run_turn.turn_id.clone();
        match message_future(self.acquire_write_locks(processor, task, run, handle.clone())).await?
        {
            TaskExecutorStartOutcome::Started => {}
            TaskExecutorStartOutcome::Queued => return Ok(()),
            TaskExecutorStartOutcome::Rejected => return Ok(()),
        }
        if let Some((_, turn)) = processor
            .crud_store
            .get_turn(child_thread_id.as_str(), child_turn_id.as_str())
            .await?
        {
            match turn.status {
                TurnStatus::Completed => {
                    self.complete_child_turn(processor, child_runtime, handle)
                        .await?;
                }
                TurnStatus::Failed | TurnStatus::Interrupted => {
                    let error_message = turn
                        .error
                        .unwrap_or_else(|| "revision child turn failed".to_owned());
                    let target_status =
                        task_run_turn_terminal_status_from_child_turn_status(turn.status)
                            .unwrap_or(TaskRunTurnStatus::Failed);
                    self.fail_child_turn(
                        child_runtime,
                        error_message.as_str(),
                        target_status,
                        handle,
                    )
                    .await?;
                }
                TurnStatus::Blocked => {
                    let error_message = turn
                        .error
                        .unwrap_or_else(|| "revision child turn blocked".to_owned());
                    self.block_child_turn(child_runtime, error_message.as_str(), handle)
                        .await?;
                }
                TurnStatus::InProgress => {
                    let started_at = now_timestamp_secs();
                    processor
                        .crud_store
                        .mark_execution_running(
                            execution.id.as_str(),
                            started_at,
                            Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                        )
                        .await
                        .context("failed to mark revision task run execution running")?;
                    processor
                        .ensure_agent_listener_task(child_thread_id.as_str())
                        .await;
                    spawn_execution_heartbeat(
                        processor,
                        execution.id.clone(),
                        child_runtime.task_run_turn.thread_id.clone(),
                        child_runtime.task_run_turn.turn_id.clone(),
                        run.id.clone(),
                    );
                }
            }
            return Ok(());
        }

        let Some(seed_thread) = processor
            .crud_store
            .get_thread_model(child_thread_id.as_str())
            .await?
        else {
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "child_thread_missing",
                    "revision child task thread is missing".to_owned(),
                    TaskErrorClass::Internal,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        };
        let seed_sandbox_mode = processor
            .crud_store
            .get_thread_sandbox_mode(child_thread_id.as_str())
            .await?;
        let parent = resolve_parent_context(processor, task).await?;
        let effective_model = effective_agent_model(agent_spec)?;
        let thread_outcome = processor
            .thread_manager
            .system_thread_start_seeded(
                task.workspace_id.clone(),
                pioneer_protocol::ThreadStartParams {
                    thread_id: child_runtime.task_run_turn.thread_id.clone(),
                    workspace_id: task.workspace_id.clone(),
                    name: thread_name_from_task(task),
                    model: Some(effective_model.model.clone()),
                    model_provider: Some(effective_model.model_provider.clone()),
                    sandbox: seed_sandbox_mode,
                    mode: Some(ThreadMode::Agent),
                    origin_kind: Some(ThreadOriginKind::TaskRun),
                    sidebar_visibility: Some(ThreadSidebarVisibility::Hidden),
                    agent_nickname: agent_spec.agent_nickname.clone(),
                    agent_role: agent_spec.agent_role.clone(),
                },
                Some(seed_thread),
                seed_sandbox_mode,
            )
            .await
            .context("failed to restore revision task thread")?;
        let child_permission_profile = effective_task_child_permission_profile(agent_spec, None)?;
        let input = materialize_child_task_input(
            materialize_child_task_prompt(
                processor,
                task_response,
                run,
                agent_spec,
                &parent,
                Some(&child_runtime.task_run_turn),
                &child_permission_profile,
            )
            .await?,
            agent_spec,
        );
        let turn_outcome = match processor
            .thread_manager
            .system_turn_start_with_permission_profile(
                TurnStartParams {
                    thread_id: child_runtime.task_run_turn.thread_id.clone(),
                    turn_id: child_runtime.task_run_turn.turn_id.clone(),
                    input,
                    capabilities: Vec::new(),
                    model: Some(effective_model.model),
                    model_provider: Some(effective_model.model_provider),
                    sandbox_policy: None,
                    mode: Some(ThreadMode::Agent),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
                },
                child_permission_profile,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) if format!("{error:#}").contains("already has a running turn") => {
                let started_at = now_timestamp_secs();
                processor
                    .crud_store
                    .mark_execution_running(
                        execution.id.as_str(),
                        started_at,
                        Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                    )
                    .await
                    .context("failed to mark revision task run execution running")?;
                processor
                    .ensure_agent_listener_task(child_thread_id.as_str())
                    .await;
                spawn_execution_heartbeat(
                    processor,
                    execution.id.clone(),
                    child_runtime.task_run_turn.thread_id,
                    child_runtime.task_run_turn.turn_id,
                    run.id.clone(),
                );
                return Ok(());
            }
            Err(error) => {
                self.block_revision_dispatch_turn(
                    processor,
                    child_runtime,
                    handle,
                    task_error(
                        "revision_turn_start_failed",
                        format!("failed to create revision task turn: {error:#}"),
                        TaskErrorClass::Internal,
                        Some(run.id.clone()),
                    ),
                )
                .await?;
                return Ok(());
            }
        };
        if let Err(error) = processor
            .validate_artifact_user_inputs(
                task.workspace_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "revision_artifact_input_invalid",
                    format!("failed to validate revision task artifact input: {error:#}"),
                    TaskErrorClass::Validation,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        }
        let turn_permission_profile = match processor
            .materialized_turn_permission_profile(&turn_outcome.materialization.turn)
        {
            Ok(permission_profile) => permission_profile,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                self.block_revision_dispatch_turn(
                    processor,
                    child_runtime,
                    handle,
                    task_error(
                        "revision_permission_profile_missing",
                        format!("failed to resolve revision task permission profile: {error:#}"),
                        TaskErrorClass::Internal,
                        Some(run.id.clone()),
                    ),
                )
                .await?;
                return Ok(());
            }
        };
        let profile_selected_audit = processor.turn_profile_selected_audit_event_for_turn(
            task.workspace_id.as_str(),
            child_thread_id.as_str(),
            child_turn_id.as_str(),
            turn_permission_profile.clone(),
        );
        if let Err(error) = processor
            .crud_store
            .materialize_turn_start_with_permission_audit(
                &turn_outcome.materialization.thread,
                turn_outcome.materialization.sandbox_mode,
                &turn_outcome.materialization.turn,
                &turn_outcome.materialization.input,
                profile_selected_audit,
            )
            .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "revision_turn_persist_failed",
                    format!("failed to persist revision task turn: {error:#}"),
                    TaskErrorClass::Internal,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        }
        processor.ensure_hook_runtime_with_run_store().await;
        processor
            .agent_manager
            .ensure_thread(child_thread_id.as_str(), task.workspace_id.as_str())
            .await
            .map_err(|error| anyhow!("failed to prepare revision child agent runtime: {error}"))?;
        processor
            .ensure_agent_listener_task(child_thread_id.as_str())
            .await;

        let started_at = now_timestamp_secs();
        processor
            .crud_store
            .mark_execution_running(
                execution.id.as_str(),
                started_at,
                Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
            )
            .await
            .context("failed to mark revision task run execution running")?;
        let workspace_skill_policies =
            load_workspace_skill_policies(processor, task.workspace_id.as_str()).await;
        let resolved_artifacts = processor
            .resolve_provider_artifact_inputs(
                task.workspace_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
            .context("failed to resolve revision task artifact input for provider")?;
        let runtime_environment = processor
            .create_artifact_output_environment(
                task.workspace_id.as_str(),
                child_thread_id.as_str(),
                child_turn_id.as_str(),
            )
            .await
            .context("failed to prepare revision task artifact output directory")?
            .into_iter()
            .collect();
        let execution_checkpoint_context =
            load_execution_checkpoint_context_for_turn(processor, child_turn_id.as_str()).await?;
        let hook_runtime_context = AgentTurnHookRuntimeContext::task(task.id.clone());
        if let Err(error) = processor
            .persist_turn_runtime_snapshot(
                child_thread_id.as_str(),
                task.workspace_id.as_str(),
                child_turn_id.as_str(),
                ThreadMode::Agent,
                &hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                None,
                &workspace_skill_policies,
                turn_outcome.materialization.input.as_slice(),
                turn_outcome.materialization.capabilities.as_slice(),
                resolved_artifacts.as_slice(),
                &runtime_environment,
                &[],
            )
            .await
        {
            processor
                .report_turn_failure(
                    child_thread_id.clone(),
                    child_turn_id.clone(),
                    TurnFailureRecoveryKind::TaskDispatch,
                    format!("failed to persist revision task turn runtime snapshot: {error:#}"),
                )
                .await;
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "revision_dispatch_snapshot_failed",
                    format!("failed to persist revision task turn runtime snapshot: {error:#}"),
                    TaskErrorClass::Internal,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        }
        let runtime_permission_profile = turn_permission_profile;
        if let Err(error) = processor
            .agent_manager
            .start_turn_with_hook_context_and_execution_checkpoint_and_permission_profile(
                child_thread_id.as_str(),
                child_turn_id.as_str(),
                ThreadMode::Agent,
                hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                workspace_skill_policies,
                turn_outcome.materialization.input,
                turn_outcome.materialization.capabilities,
                resolved_artifacts,
                runtime_environment,
                Vec::new(),
                execution_checkpoint_context,
                runtime_permission_profile,
            )
            .await
        {
            processor
                .report_turn_failure(
                    child_thread_id.clone(),
                    child_turn_id.clone(),
                    TurnFailureRecoveryKind::TaskDispatch,
                    format!("failed to dispatch revision task turn: {error}"),
                )
                .await;
            self.block_revision_dispatch_turn(
                processor,
                child_runtime,
                handle,
                task_error(
                    "revision_dispatch_failed",
                    format!("failed to dispatch revision task turn: {error:#}"),
                    TaskErrorClass::Internal,
                    Some(run.id.clone()),
                ),
            )
            .await?;
            return Ok(());
        }
        spawn_execution_heartbeat(
            processor,
            execution.id.clone(),
            child_runtime.task_run_turn.thread_id,
            child_runtime.task_run_turn.turn_id,
            run.id.clone(),
        );
        Ok(())
    }

    async fn recover_existing_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
        execution: &TaskRunExecution,
        child_runtime: TaskRunChildRuntime,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        let Some((_, turn)) = processor
            .crud_store
            .get_turn(
                child_runtime.task_run_turn.thread_id.as_str(),
                child_runtime.task_run_turn.turn_id.as_str(),
            )
            .await?
        else {
            let now = now_timestamp_secs();
            handle
                .fail_run(
                    Some(task_error(
                        "child_turn_missing",
                        "child task turn is missing during recovery".to_owned(),
                        TaskErrorClass::Internal,
                        Some(run.id.clone()),
                    )),
                    now,
                )
                .await?;
            return Ok(TaskExecutorStartOutcome::Started);
        };

        match turn.status {
            TurnStatus::Completed => {
                self.complete_child_turn(processor, child_runtime, handle)
                    .await?;
                Ok(TaskExecutorStartOutcome::Started)
            }
            TurnStatus::Failed | TurnStatus::Interrupted => {
                let error_message = turn.error.unwrap_or_else(|| "child turn failed".to_owned());
                let target_status =
                    task_run_turn_terminal_status_from_child_turn_status(turn.status)
                        .unwrap_or(TaskRunTurnStatus::Failed);
                self.fail_child_turn(child_runtime, error_message.as_str(), target_status, handle)
                    .await?;
                Ok(TaskExecutorStartOutcome::Started)
            }
            TurnStatus::Blocked => {
                let error_message = turn
                    .error
                    .unwrap_or_else(|| "child turn blocked".to_owned());
                self.block_child_turn(child_runtime, error_message.as_str(), handle)
                    .await?;
                Ok(TaskExecutorStartOutcome::Started)
            }
            TurnStatus::InProgress => {
                self.restart_in_progress_child_turn(
                    processor,
                    task_response,
                    run,
                    agent_spec,
                    execution,
                    &child_runtime,
                    None,
                    handle,
                )
                .await
            }
        }
    }

    async fn restart_in_progress_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
        execution: &TaskRunExecution,
        child_runtime: &TaskRunChildRuntime,
        launch_permission_profile: Option<&TurnPermissionProfileSnapshot>,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        let task = &task_response.task;
        let child_thread_id = child_runtime.task_run_turn.thread_id.as_str();
        let child_turn_id = child_runtime.task_run_turn.turn_id.as_str();
        match self
            .acquire_write_locks(processor, task, run, handle.clone())
            .await?
        {
            TaskExecutorStartOutcome::Started => {}
            outcome => return Ok(outcome),
        }
        let Some(seed_thread) = processor
            .crud_store
            .get_thread_model(child_thread_id)
            .await?
        else {
            let now = now_timestamp_secs();
            handle
                .fail_run(
                    Some(task_error(
                        "child_thread_missing",
                        "child task thread is missing during recovery".to_owned(),
                        TaskErrorClass::Internal,
                        Some(run.id.clone()),
                    )),
                    now,
                )
                .await?;
            return Ok(TaskExecutorStartOutcome::Started);
        };
        let seed_sandbox_mode = processor
            .crud_store
            .get_thread_sandbox_mode(child_thread_id)
            .await?;
        let parent = resolve_parent_context(processor, task).await?;
        let effective_model = effective_agent_model(agent_spec)?;
        let thread_outcome = processor
            .thread_manager
            .system_thread_start_seeded(
                task.workspace_id.clone(),
                pioneer_protocol::ThreadStartParams {
                    thread_id: child_runtime.task_run_turn.thread_id.clone(),
                    workspace_id: task.workspace_id.clone(),
                    name: thread_name_from_task(task),
                    model: Some(effective_model.model.clone()),
                    model_provider: Some(effective_model.model_provider.clone()),
                    sandbox: seed_sandbox_mode,
                    mode: Some(ThreadMode::Agent),
                    origin_kind: Some(ThreadOriginKind::TaskRun),
                    sidebar_visibility: Some(ThreadSidebarVisibility::Hidden),
                    agent_nickname: agent_spec.agent_nickname.clone(),
                    agent_role: agent_spec.agent_role.clone(),
                },
                Some(seed_thread),
                seed_sandbox_mode,
            )
            .await
            .context("failed to restore hidden task thread")?;
        let child_permission_profile =
            effective_task_child_permission_profile(agent_spec, launch_permission_profile)?;
        let input = materialize_child_task_input(
            materialize_child_task_prompt(
                processor,
                task_response,
                run,
                agent_spec,
                &parent,
                Some(&child_runtime.task_run_turn),
                &child_permission_profile,
            )
            .await?,
            agent_spec,
        );
        let turn_outcome = match processor
            .thread_manager
            .system_turn_start_with_permission_profile(
                TurnStartParams {
                    thread_id: child_runtime.task_run_turn.thread_id.clone(),
                    turn_id: child_runtime.task_run_turn.turn_id.clone(),
                    input,
                    capabilities: Vec::new(),
                    model: Some(effective_model.model),
                    model_provider: Some(effective_model.model_provider),
                    sandbox_policy: None,
                    mode: Some(ThreadMode::Agent),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
                },
                child_permission_profile,
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(error) if format!("{error:#}").contains("already has a running turn") => {
                processor.ensure_agent_listener_task(child_thread_id).await;
                spawn_execution_heartbeat(
                    processor,
                    execution.id.clone(),
                    child_runtime.task_run_turn.thread_id.clone(),
                    child_runtime.task_run_turn.turn_id.clone(),
                    run.id.clone(),
                );
                return Ok(TaskExecutorStartOutcome::Queued);
            }
            Err(error) => return Err(error).context("failed to restore hidden task turn"),
        };

        if let Err(error) = processor
            .validate_artifact_user_inputs(
                task.workspace_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).context("failed to validate restored task artifact input");
        }

        processor.ensure_hook_runtime_with_run_store().await;
        processor
            .agent_manager
            .ensure_thread(child_thread_id, task.workspace_id.as_str())
            .await
            .map_err(|error| anyhow!("failed to restore child agent runtime: {error}"))?;
        processor.ensure_agent_listener_task(child_thread_id).await;

        if run.status != TaskRunStatus::Running
            || execution.status != TaskRunExecutionStatus::Running
        {
            let started_at = now_timestamp_secs();
            handle.mark_started(started_at).await?;
            processor
                .crud_store
                .mark_execution_running(
                    execution.id.as_str(),
                    started_at,
                    Some(started_at.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                )
                .await
                .context("failed to mark restored task run execution running")?;
        }
        let workspace_skill_policies =
            load_workspace_skill_policies(processor, task.workspace_id.as_str()).await;
        let resolved_artifacts = processor
            .resolve_provider_artifact_inputs(
                task.workspace_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
            .context("failed to resolve restored task artifact input for provider")?;
        let runtime_environment = processor
            .create_artifact_output_environment(
                task.workspace_id.as_str(),
                child_thread_id,
                child_turn_id,
            )
            .await
            .context("failed to prepare restored task artifact output directory")?
            .into_iter()
            .collect();
        let execution_checkpoint_context =
            load_execution_checkpoint_context_for_turn(processor, child_turn_id).await?;
        let hook_runtime_context = AgentTurnHookRuntimeContext::task(task.id.clone());
        processor
            .persist_turn_runtime_snapshot(
                child_thread_id,
                task.workspace_id.as_str(),
                child_turn_id,
                ThreadMode::Agent,
                &hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                None,
                &workspace_skill_policies,
                turn_outcome.materialization.input.as_slice(),
                turn_outcome.materialization.capabilities.as_slice(),
                resolved_artifacts.as_slice(),
                &runtime_environment,
                &[],
            )
            .await
            .context("failed to persist restored task turn runtime snapshot")?;
        let runtime_permission_profile = processor
            .materialized_turn_permission_profile(&turn_outcome.materialization.turn)
            .context("failed to resolve restored task permission profile")?;
        processor
            .agent_manager
            .start_turn_with_hook_context_and_execution_checkpoint_and_permission_profile(
                child_thread_id,
                child_turn_id,
                ThreadMode::Agent,
                hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                workspace_skill_policies,
                turn_outcome.materialization.input,
                turn_outcome.materialization.capabilities,
                resolved_artifacts,
                runtime_environment,
                Vec::new(),
                execution_checkpoint_context,
                runtime_permission_profile,
            )
            .await
            .map_err(|error| anyhow!("failed to redispatch child task turn: {error}"))?;
        spawn_execution_heartbeat(
            processor,
            execution.id.clone(),
            child_runtime.task_run_turn.thread_id.clone(),
            child_runtime.task_run_turn.turn_id.clone(),
            run.id.clone(),
        );

        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn acquire_write_locks(
        &self,
        processor: &Arc<MessageProcessor>,
        task: &Task,
        run: &TaskRun,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        match processor
            .task_runtime
            .service()
            .acquire_write_locks_for_run(run.id.as_str(), now_timestamp_secs())
            .await?
        {
            WriteLockDecision::NoLocksRequired | WriteLockDecision::Acquired(_) => {
                Ok(TaskExecutorStartOutcome::Started)
            }
            WriteLockDecision::Queued => Ok(TaskExecutorStartOutcome::Queued),
            WriteLockDecision::Rejected => {
                handle
                    .fail_run(
                        Some(task_error(
                            "write_lock_conflict",
                            "write scope conflicts with another active task run".to_owned(),
                            TaskErrorClass::Policy,
                            Some(run.id.clone()),
                        )),
                        now_timestamp_secs(),
                    )
                    .await?;
                let _ = task;
                Ok(TaskExecutorStartOutcome::Started)
            }
        }
    }

    pub(super) async fn reconcile_child_turn_completed(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<bool> {
        let processor = self.processor()?;
        let Some(child_runtime) =
            load_child_runtime_for_turn(&processor, thread_id, turn_id).await?
        else {
            return Ok(false);
        };
        let Some(task_response) = processor
            .crud_store
            .get_task(child_runtime.task_run_turn.task_id.as_str())
            .await?
        else {
            return Ok(true);
        };
        if task_response.task.status.is_terminal() {
            return Ok(true);
        }
        let handle = TaskExecutionHandle::new(
            processor.crud_store.clone(),
            processor.task_runtime.event_bus(),
            child_runtime.task_run_turn.task_id.clone(),
            child_runtime.task_run_turn.run_id.clone(),
        );
        self.complete_child_turn(&processor, child_runtime, handle)
            .await?;
        Ok(true)
    }

    pub(super) async fn reconcile_child_turn_failed(
        &self,
        thread_id: &str,
        turn_id: &str,
        error_message: &str,
    ) -> Result<bool> {
        let processor = self.processor()?;
        let Some(child_runtime) =
            load_child_runtime_for_turn(&processor, thread_id, turn_id).await?
        else {
            return Ok(false);
        };
        let Some(task_response) = processor
            .crud_store
            .get_task(child_runtime.task_run_turn.task_id.as_str())
            .await?
        else {
            return Ok(true);
        };
        if task_response.task.status.is_terminal() {
            return Ok(true);
        }
        let handle = TaskExecutionHandle::new(
            processor.crud_store.clone(),
            processor.task_runtime.event_bus(),
            child_runtime.task_run_turn.task_id.clone(),
            child_runtime.task_run_turn.run_id.clone(),
        );
        if child_runtime.task_run_turn.kind == TaskRunTurnKind::Review {
            let failed_at = now_timestamp_secs();
            record_task_run_turn_failure(
                &handle,
                &child_runtime.task_run_turn,
                TaskRunTurnStatus::Failed,
                Some(task_error(
                    "reviewer_turn_failed",
                    error_message.to_owned(),
                    TaskErrorClass::Unknown,
                    Some(child_runtime.task_run_turn.run_id.clone()),
                )),
                failed_at,
            )
            .await?;
            return Ok(true);
        }
        let Some((_, turn)) = processor.crud_store.get_turn(thread_id, turn_id).await? else {
            self.fail_child_turn(
                child_runtime,
                error_message,
                TaskRunTurnStatus::Failed,
                handle,
            )
            .await?;
            return Ok(true);
        };
        if turn.status == TurnStatus::Blocked {
            self.block_child_turn(child_runtime, error_message, handle)
                .await?;
            return Ok(true);
        }
        let target_status = task_run_turn_terminal_status_from_child_turn_status(turn.status)
            .unwrap_or(TaskRunTurnStatus::Failed);
        self.fail_child_turn(child_runtime, error_message, target_status, handle)
            .await?;
        Ok(true)
    }

    pub(super) async fn reconcile_child_turn_blocked(
        &self,
        thread_id: &str,
        turn_id: &str,
        reason: &str,
    ) -> Result<bool> {
        let processor = self.processor()?;
        let Some(child_runtime) =
            load_child_runtime_for_turn(&processor, thread_id, turn_id).await?
        else {
            return Ok(false);
        };
        let Some(task_response) = processor
            .crud_store
            .get_task(child_runtime.task_run_turn.task_id.as_str())
            .await?
        else {
            return Ok(true);
        };
        if task_response.task.status.is_terminal() {
            return Ok(true);
        }
        let handle = TaskExecutionHandle::new(
            processor.crud_store.clone(),
            processor.task_runtime.event_bus(),
            child_runtime.task_run_turn.task_id.clone(),
            child_runtime.task_run_turn.run_id.clone(),
        );
        self.block_child_turn(child_runtime, reason, handle).await?;
        Ok(true)
    }

    async fn complete_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        child_runtime: TaskRunChildRuntime,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        if child_runtime.task_run_turn.kind == TaskRunTurnKind::Review {
            self.complete_reviewer_turn(processor, child_runtime, handle)
                .await?;
            return Ok(());
        }
        let task_response = processor
            .crud_store
            .get_task(child_runtime.task_run_turn.task_id.as_str())
            .await?
            .ok_or_else(|| anyhow!("task `{}` not found", child_runtime.task_run_turn.task_id))?;
        let agent_spec =
            select_agent_spec(&task_response, child_runtime.task_run_turn.run_id.as_str())
                .ok_or_else(|| {
                    anyhow!("agent task `{}` has no agent spec", task_response.task.id)
                })?;
        let review_policy = agent_spec
            .review_policy
            .clone()
            .filter(|policy| policy.is_enabled());
        if let Some(candidate) = processor
            .crud_store
            .get_accepted_task_result_candidate(child_runtime.task_run_turn.run_id.as_str())
            .await?
            && candidate.task_run_turn_id == child_runtime.task_run_turn.id
            && let Some(result) = candidate.result
        {
            handle
                .complete_run(
                    Some(result),
                    candidate.resolved_at.unwrap_or_else(now_timestamp_secs),
                )
                .await?;
            mark_task_run_occurrence_turn_completed(processor, &child_runtime.lineage).await?;
            return Ok(());
        }
        if let Some(candidate) = processor
            .crud_store
            .get_task_result_candidate_by_turn(child_runtime.task_run_turn.id.as_str())
            .await?
            && matches!(
                candidate.status,
                TaskResultCandidateStatus::PendingReview
                    | TaskResultCandidateStatus::ExtractionFailed
                    | TaskResultCandidateStatus::Rejected
                    | TaskResultCandidateStatus::Superseded
                    | TaskResultCandidateStatus::Cancelled
            )
        {
            return Ok(());
        }

        let artifact_mode = if review_policy.is_some() {
            TaskAgentResultArtifactMode::ResultCandidate {
                candidate_id: task_result_candidate_id(
                    child_runtime.task_run_turn.run_id.as_str(),
                    child_runtime.task_run_turn.turn_id.as_str(),
                ),
            }
        } else {
            TaskAgentResultArtifactMode::FinalResult
        };
        match TaskAgentResultExtractor::extract_with_artifact_mode(
            processor,
            &child_runtime.task_run_turn,
            &child_runtime.lineage,
            artifact_mode,
        )
        .await?
        {
            Ok(result) if review_policy.is_some() => {
                let review_policy = review_policy.as_ref().expect("review policy checked");
                let completed_at = now_timestamp_secs();
                let completed_turn =
                    candidate_created_task_run_turn(&child_runtime.task_run_turn, completed_at);
                let candidate = match invalid_structured_result_error(
                    &result,
                    &agent_spec,
                    child_runtime.task_run_turn.run_id.as_str(),
                ) {
                    Some(error) if revision_possible(review_policy, &completed_turn) => {
                        extraction_failed_result_candidate(&completed_turn, error, completed_at)
                    }
                    Some(error) => {
                        record_task_run_turn_failure(
                            &handle,
                            &child_runtime.task_run_turn,
                            TaskRunTurnStatus::Failed,
                            Some(error.clone()),
                            completed_at,
                        )
                        .await?;
                        handle.fail_run(Some(error), completed_at).await?;
                        mark_task_run_occurrence_turn_failed(
                            processor,
                            &child_runtime.lineage,
                            "child task result extraction failed",
                        )
                        .await?;
                        return Ok(());
                    }
                    None => pending_review_result_candidate(&completed_turn, result, completed_at),
                };
                let candidate_for_review = candidate.clone();
                handle
                    .record_pending_review_result_candidate(completed_turn, candidate, completed_at)
                    .await?;
                self.start_reviewer_turns_for_candidate(
                    processor,
                    &task_response,
                    &agent_spec,
                    review_policy,
                    &candidate_for_review,
                )
                .await?;
            }
            Ok(result) => {
                let completed_at = now_timestamp_secs();
                let completed_turn =
                    candidate_created_task_run_turn(&child_runtime.task_run_turn, completed_at);
                let candidate =
                    accepted_result_candidate(&completed_turn, result.clone(), completed_at);
                let review_event = runtime_auto_accept_review_event(&candidate, completed_at);
                handle
                    .record_auto_accepted_result_candidate(
                        completed_turn,
                        candidate,
                        review_event,
                        completed_at,
                    )
                    .await?;
                handle.complete_run(Some(result), completed_at).await?;
                mark_task_run_occurrence_turn_completed(processor, &child_runtime.lineage).await?;
            }
            Err(error)
                if review_policy.as_ref().is_some_and(|policy| {
                    revision_possible(policy, &child_runtime.task_run_turn)
                }) =>
            {
                let completed_at = now_timestamp_secs();
                let completed_turn =
                    candidate_created_task_run_turn(&child_runtime.task_run_turn, completed_at);
                let candidate =
                    extraction_failed_result_candidate(&completed_turn, error, completed_at);
                let candidate_for_review = candidate.clone();
                handle
                    .record_pending_review_result_candidate(completed_turn, candidate, completed_at)
                    .await?;
                let review_policy = review_policy.as_ref().expect("review policy checked");
                self.start_reviewer_turns_for_candidate(
                    processor,
                    &task_response,
                    &agent_spec,
                    review_policy,
                    &candidate_for_review,
                )
                .await?;
            }
            Err(error) => {
                let failed_at = now_timestamp_secs();
                record_task_run_turn_failure(
                    &handle,
                    &child_runtime.task_run_turn,
                    TaskRunTurnStatus::Failed,
                    Some(error.clone()),
                    failed_at,
                )
                .await?;
                handle.fail_run(Some(error), failed_at).await?;
                mark_task_run_occurrence_turn_failed(
                    processor,
                    &child_runtime.lineage,
                    "child task result extraction failed",
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn start_reviewer_turns_for_candidate(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        agent_spec: &TaskAgentSpec,
        review_policy: &TaskAgentReviewPolicy,
        candidate: &TaskResultCandidate,
    ) -> Result<()> {
        if review_policy.reviewers.is_empty() {
            return Ok(());
        }
        for (index, reviewer_spec) in review_policy.reviewers.iter().enumerate() {
            if reviewer_spec.reviewer_kind != TaskResultReviewerKind::ReviewAgent {
                continue;
            }
            let reviewer_key = task_result_reviewer_spec_key(index, reviewer_spec);
            let reviewer_thread_id =
                stable_review_thread_id(candidate.id.as_str(), reviewer_key.as_str());
            let reviewer_turn_id =
                stable_review_turn_id(candidate.id.as_str(), reviewer_key.as_str());
            let reviewer_context = processor
                .task_runtime
                .service()
                .create_task_result_reviewer_context(CreateTaskResultReviewerContextParams {
                    candidate_id: candidate.id.clone(),
                    reviewer_index: index,
                    reviewer_spec: reviewer_spec.clone(),
                    reviewer_thread_id,
                    reviewer_turn_id,
                    created_at: Some(now_timestamp_secs()),
                })
                .await?;
            self.dispatch_or_recover_reviewer_turn(
                processor,
                task_response,
                agent_spec,
                review_policy,
                candidate,
                index,
                reviewer_spec,
                reviewer_context.task_run_turn,
            )
            .await?;
        }
        Ok(())
    }

    async fn dispatch_or_recover_reviewer_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        agent_spec: &TaskAgentSpec,
        review_policy: &TaskAgentReviewPolicy,
        candidate: &TaskResultCandidate,
        reviewer_index: usize,
        reviewer_spec: &TaskResultReviewerSpec,
        task_run_turn: TaskRunTurn,
    ) -> Result<()> {
        if self
            .review_event_exists_for_turn(candidate.id.as_str(), task_run_turn.turn_id.as_str())
            .await?
        {
            return Ok(());
        }
        if let Some((_, turn)) = processor
            .crud_store
            .get_turn(
                task_run_turn.thread_id.as_str(),
                task_run_turn.turn_id.as_str(),
            )
            .await?
        {
            match turn.status {
                TurnStatus::Completed => {
                    let handle = TaskExecutionHandle::new(
                        processor.crud_store.clone(),
                        processor.task_runtime.event_bus(),
                        task_run_turn.task_id.clone(),
                        task_run_turn.run_id.clone(),
                    );
                    self.complete_reviewer_turn(
                        processor,
                        TaskRunChildRuntime {
                            lineage: processor
                                .crud_store
                                .get_task_thread_lineage(task_run_turn.thread_id.as_str())
                                .await?
                                .unwrap_or_else(|| {
                                    fallback_lineage_for_task_run_turn(&task_run_turn)
                                }),
                            task_run_turn,
                        },
                        handle,
                    )
                    .await?;
                }
                TurnStatus::InProgress => {}
                TurnStatus::Failed | TurnStatus::Interrupted => {
                    let handle = TaskExecutionHandle::new(
                        processor.crud_store.clone(),
                        processor.task_runtime.event_bus(),
                        task_run_turn.task_id.clone(),
                        task_run_turn.run_id.clone(),
                    );
                    let error_message = turn
                        .error
                        .unwrap_or_else(|| "reviewer child turn failed".to_owned());
                    let target_status =
                        task_run_turn_terminal_status_from_child_turn_status(turn.status)
                            .unwrap_or(TaskRunTurnStatus::Failed);
                    let failed_at = now_timestamp_secs();
                    record_task_run_turn_failure(
                        &handle,
                        &task_run_turn,
                        target_status,
                        Some(task_error(
                            "reviewer_turn_failed",
                            error_message,
                            TaskErrorClass::Unknown,
                            Some(task_run_turn.run_id.clone()),
                        )),
                        failed_at,
                    )
                    .await?;
                }
                TurnStatus::Blocked => {
                    let handle = TaskExecutionHandle::new(
                        processor.crud_store.clone(),
                        processor.task_runtime.event_bus(),
                        task_run_turn.task_id.clone(),
                        task_run_turn.run_id.clone(),
                    );
                    let error_message = turn
                        .error
                        .unwrap_or_else(|| "reviewer child turn blocked".to_owned());
                    self.block_child_turn(
                        TaskRunChildRuntime {
                            lineage: processor
                                .crud_store
                                .get_task_thread_lineage(task_run_turn.thread_id.as_str())
                                .await?
                                .unwrap_or_else(|| {
                                    fallback_lineage_for_task_run_turn(&task_run_turn)
                                }),
                            task_run_turn,
                        },
                        error_message.as_str(),
                        handle,
                    )
                    .await?;
                }
            }
            return Ok(());
        }

        let task = &task_response.task;
        let effective_model = effective_agent_model(agent_spec)?;
        let thread_params = pioneer_protocol::ThreadStartParams {
            thread_id: task_run_turn.thread_id.clone(),
            workspace_id: task.workspace_id.clone(),
            name: Some(reviewer_thread_name(task, reviewer_spec)),
            model: Some(effective_model.model.clone()),
            model_provider: Some(effective_model.model_provider.clone()),
            sandbox: Some(SandboxMode::FullAccess),
            mode: Some(ThreadMode::Agent),
            origin_kind: Some(ThreadOriginKind::TaskRun),
            sidebar_visibility: Some(ThreadSidebarVisibility::Hidden),
            agent_nickname: reviewer_spec.agent_nickname.clone(),
            agent_role: reviewer_spec.agent_role.clone(),
        };
        let thread_outcome = processor
            .thread_manager
            .system_thread_start_seeded(task.workspace_id.clone(), thread_params, None, None)
            .await
            .context("failed to create hidden reviewer thread")?;
        let reviewer_key = task_result_reviewer_spec_key(reviewer_index, reviewer_spec);
        let prompt = materialize_reviewer_prompt(
            task_response,
            agent_spec,
            review_policy,
            candidate,
            reviewer_spec,
            reviewer_key.as_str(),
        );
        let input = vec![UserInput::Text {
            text: prompt,
            text_elements: Vec::new(),
        }];
        let reviewer_permission_profile =
            effective_task_child_permission_profile(agent_spec, None)?;
        let turn_outcome = processor
            .thread_manager
            .system_turn_start_with_permission_profile(
                TurnStartParams {
                    thread_id: task_run_turn.thread_id.clone(),
                    turn_id: task_run_turn.turn_id.clone(),
                    input,
                    capabilities: Vec::new(),
                    model: Some(effective_model.model),
                    model_provider: Some(effective_model.model_provider),
                    sandbox_policy: None,
                    mode: Some(ThreadMode::Agent),
                    execution_backend: None,
                    reasoning: None,
                    permission_profile: None,
                    cli_runtime_options: None,
                },
                reviewer_permission_profile,
            )
            .await
            .context("failed to create hidden reviewer turn")?;

        if let Err(error) = processor
            .validate_artifact_user_inputs(
                task.workspace_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).context("failed to validate hidden reviewer input");
        }
        let turn_permission_profile = match processor
            .materialized_turn_permission_profile(&turn_outcome.materialization.turn)
        {
            Ok(permission_profile) => permission_profile,
            Err(error) => {
                processor
                    .thread_manager
                    .rollback_turn_start(turn_outcome.rollback_context)
                    .await;
                return Err(error).context("failed to resolve reviewer task permission profile");
            }
        };
        let profile_selected_audit = processor.turn_profile_selected_audit_event_for_turn(
            task.workspace_id.as_str(),
            task_run_turn.thread_id.as_str(),
            task_run_turn.turn_id.as_str(),
            turn_permission_profile.clone(),
        );
        if let Err(error) = processor
            .crud_store
            .materialize_turn_start_with_permission_audit(
                &turn_outcome.materialization.thread,
                turn_outcome.materialization.sandbox_mode,
                &turn_outcome.materialization.turn,
                &turn_outcome.materialization.input,
                profile_selected_audit,
            )
            .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).context("failed to persist hidden reviewer turn");
        }

        processor.ensure_hook_runtime_with_run_store().await;
        processor
            .agent_manager
            .ensure_thread(task_run_turn.thread_id.as_str(), task.workspace_id.as_str())
            .await
            .map_err(|error| anyhow!("failed to prepare reviewer agent runtime: {error}"))?;
        let workspace_skill_policies =
            load_workspace_skill_policies(processor, task.workspace_id.as_str()).await;
        let resolved_artifacts = processor
            .resolve_provider_artifact_inputs(
                task.workspace_id.as_str(),
                turn_outcome.materialization.input.as_slice(),
            )
            .await
            .context("failed to resolve reviewer artifact input for provider")?;
        let runtime_environment = processor
            .create_artifact_output_environment(
                task.workspace_id.as_str(),
                task_run_turn.thread_id.as_str(),
                task_run_turn.turn_id.as_str(),
            )
            .await
            .context("failed to prepare reviewer artifact output directory")?
            .into_iter()
            .collect();
        let hook_runtime_context = AgentTurnHookRuntimeContext::task(task.id.clone());
        if let Err(error) = processor
            .persist_turn_runtime_snapshot(
                task_run_turn.thread_id.as_str(),
                task.workspace_id.as_str(),
                task_run_turn.turn_id.as_str(),
                ThreadMode::Agent,
                &hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                None,
                &workspace_skill_policies,
                turn_outcome.materialization.input.as_slice(),
                turn_outcome.materialization.capabilities.as_slice(),
                resolved_artifacts.as_slice(),
                &runtime_environment,
                &[],
            )
            .await
        {
            processor
                .report_turn_failure(
                    task_run_turn.thread_id,
                    task_run_turn.turn_id,
                    TurnFailureRecoveryKind::TaskDispatch,
                    format!("failed to persist reviewer task turn runtime snapshot: {error:#}"),
                )
                .await;
            return Ok(());
        }
        let runtime_permission_profile = turn_permission_profile;
        if let Err(error) = processor
            .agent_manager
            .start_turn_with_hook_context_and_permission_profile(
                task_run_turn.thread_id.as_str(),
                task_run_turn.turn_id.as_str(),
                ThreadMode::Agent,
                hook_runtime_context,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                workspace_skill_policies,
                turn_outcome.materialization.input,
                turn_outcome.materialization.capabilities,
                resolved_artifacts,
                runtime_environment,
                Vec::new(),
                runtime_permission_profile,
            )
            .await
        {
            processor
                .report_turn_failure(
                    task_run_turn.thread_id,
                    task_run_turn.turn_id,
                    TurnFailureRecoveryKind::TaskDispatch,
                    format!("failed to dispatch reviewer task turn: {error}"),
                )
                .await;
        }
        Ok(())
    }

    async fn complete_reviewer_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        reviewer_runtime: TaskRunChildRuntime,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        let Some(candidate_id) = reviewer_runtime.task_run_turn.reviews_candidate_id.clone() else {
            return Ok(());
        };
        if self
            .review_event_exists_for_turn(
                candidate_id.as_str(),
                reviewer_runtime.task_run_turn.turn_id.as_str(),
            )
            .await?
        {
            self.mark_reviewer_turn_recorded(handle, reviewer_runtime.task_run_turn)
                .await?;
            return Ok(());
        }
        let Some(candidate) = processor
            .crud_store
            .get_task_result_candidate(candidate_id.as_str())
            .await?
        else {
            return Ok(());
        };
        let task_response = processor
            .crud_store
            .get_task(candidate.task_id.as_str())
            .await?
            .ok_or_else(|| anyhow!("task `{}` not found", candidate.task_id))?;
        let agent_spec = select_agent_spec(&task_response, candidate.run_id.as_str())
            .ok_or_else(|| anyhow!("agent task `{}` has no agent spec", task_response.task.id))?;
        let reviewer_key = reviewer_key_for_turn(
            agent_spec.review_policy.as_ref(),
            &candidate,
            &reviewer_runtime.task_run_turn,
        );
        let advisory =
            extract_reviewer_advisory(processor, reviewer_runtime.task_run_turn.turn_id.as_str())
                .await?;
        processor
            .task_runtime
            .service()
            .record_task_result_review_event(RecordTaskResultReviewEventParams {
                candidate_id: candidate.id,
                review_event_id: Some(format!("trre_{}", reviewer_runtime.task_run_turn.turn_id)),
                actor: TaskResultReviewActor {
                    reviewer_kind: TaskResultReviewerKind::ReviewAgent,
                    reviewer_thread_id: Some(reviewer_runtime.task_run_turn.thread_id.clone()),
                    reviewer_turn_id: Some(reviewer_runtime.task_run_turn.turn_id.clone()),
                    reviewer_user_id: None,
                    reviewer_agent_spec_id: reviewer_key,
                },
                event_kind: TaskResultReviewEventKind::Advisory,
                decision: advisory.decision,
                feedback_text: advisory.feedback_text,
                feedback: advisory.feedback,
                confidence: advisory.confidence,
                supersedes_review_event_id: None,
                next_task_run_turn_id: None,
                created_at: Some(now_timestamp_secs()),
            })
            .await?;
        self.mark_reviewer_turn_recorded(handle, reviewer_runtime.task_run_turn)
            .await?;
        Ok(())
    }

    async fn review_event_exists_for_turn(
        &self,
        candidate_id: &str,
        turn_id: &str,
    ) -> Result<bool> {
        let processor = self.processor()?;
        Ok(processor
            .crud_store
            .list_task_result_review_events(candidate_id)
            .await?
            .iter()
            .any(|event| event.reviewer_turn_id.as_deref() == Some(turn_id)))
    }

    async fn mark_reviewer_turn_recorded(
        &self,
        handle: TaskExecutionHandle,
        mut task_run_turn: TaskRunTurn,
    ) -> Result<()> {
        if task_run_turn.status == TaskRunTurnStatus::ReviewRecorded {
            return Ok(());
        }
        let completed_at = now_timestamp_secs();
        task_run_turn.status = TaskRunTurnStatus::ReviewRecorded;
        task_run_turn.completed_at = Some(completed_at);
        handle
            .record_task_run_turn_completed(task_run_turn, completed_at)
            .await?;
        Ok(())
    }

    async fn fail_child_turn(
        &self,
        child_runtime: TaskRunChildRuntime,
        error_message: &str,
        target_status: TaskRunTurnStatus,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        let processor = self.processor()?;
        let failed_at = now_timestamp_secs();
        let error = task_error(
            "child_turn_failed",
            error_message.to_owned(),
            TaskErrorClass::Unknown,
            Some(child_runtime.task_run_turn.run_id.clone()),
        );
        record_task_run_turn_failure(
            &handle,
            &child_runtime.task_run_turn,
            target_status,
            Some(error.clone()),
            failed_at,
        )
        .await?;
        handle.fail_run(Some(error), failed_at).await?;
        mark_task_run_occurrence_turn_failed(&processor, &child_runtime.lineage, error_message)
            .await?;
        Ok(())
    }

    async fn block_child_turn(
        &self,
        child_runtime: TaskRunChildRuntime,
        reason: &str,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        let processor = self.processor()?;
        let blocked_at = now_timestamp_secs();
        let error = task_error(
            "child_turn_blocked",
            reason.to_owned(),
            TaskErrorClass::Policy,
            Some(child_runtime.task_run_turn.run_id.clone()),
        );
        handle
            .record_task_run_turn_blocked(
                blocked_task_run_turn(&child_runtime.task_run_turn, blocked_at),
                Some(error.clone()),
                blocked_at,
            )
            .await?;
        handle.block_run(Some(error), blocked_at).await?;
        mark_task_run_occurrence_turn_blocked(&processor, &child_runtime.lineage, reason).await?;
        Ok(())
    }

    async fn block_revision_dispatch_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        child_runtime: TaskRunChildRuntime,
        handle: TaskExecutionHandle,
        mut error: TaskError,
    ) -> Result<()> {
        let blocked_at = now_timestamp_secs();
        error.details = Some(revision_dispatch_error_details(
            &child_runtime.task_run_turn,
        ));
        let message = error.message.clone();
        handle
            .record_task_run_turn_blocked(
                blocked_task_run_turn(&child_runtime.task_run_turn, blocked_at),
                Some(error.clone()),
                blocked_at,
            )
            .await?;
        handle.block_run(Some(error), blocked_at).await?;
        mark_task_run_occurrence_turn_blocked(processor, &child_runtime.lineage, message.as_str())
            .await?;
        Ok(())
    }
}

#[async_trait]
impl TaskExecutor for TaskAgentExecutor {
    fn kind(&self) -> TaskExecutorKind {
        TaskExecutorKind::Agent
    }

    async fn start_run(
        &self,
        context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> pioneer_tasks::TaskRuntimeResult<TaskExecutorStartOutcome> {
        self.start_or_recover_run(context, run, handle).await
    }

    async fn cancel_run(
        &self,
        _context: TaskExecutionContext,
        run_id: &str,
        reason: &str,
        handle: TaskExecutionHandle,
    ) -> pioneer_tasks::TaskRuntimeResult<()> {
        let processor = self.processor()?;
        let child_runtimes = list_child_runtimes_for_run(&processor, run_id).await?;
        for child_runtime in child_runtimes {
            let _ = processor
                .agent_manager
                .cancel_turn(
                    child_runtime.task_run_turn.thread_id.as_str(),
                    child_runtime.task_run_turn.turn_id.as_str(),
                    reason,
                )
                .await;
            let cancelled_at = now_timestamp_secs();
            let error = task_error(
                "task_run_cancelled",
                reason.to_owned(),
                TaskErrorClass::Cancelled,
                Some(run_id.to_owned()),
            );
            record_task_run_turn_failure(
                &handle,
                &child_runtime.task_run_turn,
                TaskRunTurnStatus::Cancelled,
                Some(error),
                cancelled_at,
            )
            .await?;
            if child_runtime.task_run_turn.kind != TaskRunTurnKind::Review {
                mark_task_run_occurrence_turn_failed(&processor, &child_runtime.lineage, reason)
                    .await?;
            }
        }
        if let Some(execution) = processor.crud_store.load_execution_for_run(run_id).await?
            && !execution.status.is_terminal()
        {
            let error = task_error(
                "task_run_cancelled",
                reason.to_owned(),
                TaskErrorClass::Cancelled,
                Some(run_id.to_owned()),
            );
            let _ = processor
                .crud_store
                .mark_execution_terminal(
                    execution.id.as_str(),
                    pioneer_protocol::TaskRunExecutionStatus::Cancelled,
                    now_timestamp_secs(),
                    None,
                    Some(&error),
                )
                .await?;
        }
        Ok(())
    }

    async fn recover_run(
        &self,
        context: TaskExecutionContext,
        run: TaskRun,
        handle: TaskExecutionHandle,
    ) -> pioneer_tasks::TaskRuntimeResult<TaskExecutorRecoveryOutcome> {
        match self.start_or_recover_run(context, run, handle).await? {
            TaskExecutorStartOutcome::Started => Ok(TaskExecutorRecoveryOutcome::Recovered),
            TaskExecutorStartOutcome::Queued => Ok(TaskExecutorRecoveryOutcome::AlreadyRunning),
            TaskExecutorStartOutcome::Rejected => Ok(TaskExecutorRecoveryOutcome::LeftUnchanged),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct TaskParentRuntimeContext {
    pub(super) parent_thread_id: String,
    pub(super) parent_turn_id: Option<String>,
    pub(super) root_thread_id: String,
}

#[derive(Debug, Clone)]
struct EffectiveAgentModel {
    model: String,
    model_provider: String,
}

async fn resolve_parent_context(
    processor: &Arc<MessageProcessor>,
    task: &Task,
) -> Result<TaskParentRuntimeContext> {
    let mut root_thread_id = task
        .created_by_thread_id
        .clone()
        .or_else(|| {
            (task.owner_kind == pioneer_protocol::TaskOwnerKind::Thread)
                .then(|| task.owner_id.clone())
                .flatten()
        })
        .unwrap_or_else(|| task.id.clone());

    if let Some(parent_task_id) = task.parent_task_id.as_deref()
        && let Some(parent_response) = processor.crud_store.get_task(parent_task_id).await?
        && let Some(parent_lineage) = parent_response.thread_lineage.last()
    {
        root_thread_id = parent_lineage.root_thread_id.clone();
    }

    let parent_thread_id = task
        .created_by_thread_id
        .clone()
        .unwrap_or_else(|| root_thread_id.clone());

    Ok(TaskParentRuntimeContext {
        parent_thread_id,
        parent_turn_id: task.created_by_turn_id.clone(),
        root_thread_id,
    })
}

async fn ensure_task_run_occurrence_context(
    processor: &Arc<MessageProcessor>,
    task_response: &TaskGetResponse,
    run: &TaskRun,
    mut parent: TaskParentRuntimeContext,
    permission_profile: &TurnPermissionProfileSnapshot,
) -> Result<TaskParentRuntimeContext> {
    let Some(origin) = task_run_occurrence_origin(task_response, run) else {
        return Ok(parent);
    };
    ensure_task_run_occurrence_turn(
        processor,
        &task_response.task,
        parent.parent_thread_id.as_str(),
        run,
        origin,
        permission_profile,
    )
    .await?;
    ensure_task_run_occurrence_anchor(
        processor,
        task_response,
        parent.parent_thread_id.as_str(),
        run.id.as_str(),
    )
    .await?;
    parent.parent_turn_id = Some(run.id.clone());
    Ok(parent)
}

fn task_run_occurrence_origin(
    task_response: &TaskGetResponse,
    run: &TaskRun,
) -> Option<TurnOrigin> {
    let trigger_kind = run
        .trigger_id
        .as_deref()
        .and_then(|trigger_id| find_task_run_trigger(task_response, trigger_id))
        .map(TaskTrigger::kind);
    let attachment = task_response
        .task
        .lifecycle_policy
        .as_ref()
        .map(|policy| policy.attachment)
        .unwrap_or(TaskAttachmentMode::Detached);
    let immediate_attached_to_live_parent = trigger_kind == Some(TaskTriggerKind::Immediate)
        && attachment == TaskAttachmentMode::Attached
        && task_response.task.created_by_turn_id.is_some();
    if immediate_attached_to_live_parent {
        return None;
    }
    if matches!(
        trigger_kind,
        Some(TaskTriggerKind::ScheduledAt | TaskTriggerKind::Interval | TaskTriggerKind::Cron)
    ) {
        return Some(TurnOrigin::ScheduledTask);
    }
    if attachment == TaskAttachmentMode::Detached {
        return Some(TurnOrigin::DetachedTask);
    }
    Some(TurnOrigin::AttachedTask)
}

async fn ensure_task_run_occurrence_turn(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    parent_thread_id: &str,
    run: &TaskRun,
    origin: TurnOrigin,
    permission_profile: &TurnPermissionProfileSnapshot,
) -> Result<()> {
    if processor
        .crud_store
        .get_turn(parent_thread_id, run.id.as_str())
        .await?
        .is_some()
    {
        return Ok(());
    }

    let Some(mut parent_thread) = processor
        .crud_store
        .get_thread_model(parent_thread_id)
        .await?
    else {
        bail!(
            "cannot create task run occurrence turn for task `{}` without parent thread `{}`",
            task.id,
            parent_thread_id
        );
    };
    let now = now_timestamp_secs();
    parent_thread.updated_at = now;
    parent_thread.status = ThreadStatus::Active;
    parent_thread.turns.clear();
    let occurrence_turn = Turn {
        id: run.id.clone(),
        status: TurnStatus::InProgress,
        turn_kind: TurnKind::TaskRun,
        origin,
        error: None,
        prompt_manifest: None,
        permission_profile: permission_profile.clone(),
    };
    let sandbox_mode = processor
        .crud_store
        .get_thread_sandbox_mode(parent_thread_id)
        .await?
        .unwrap_or(SandboxMode::FullAccess);
    let profile_selected_audit = processor.turn_profile_selected_audit_event_for_turn(
        task.workspace_id.as_str(),
        parent_thread_id,
        run.id.as_str(),
        permission_profile.clone(),
    );
    processor
        .crud_store
        .materialize_turn_start_with_permission_audit(
            &parent_thread,
            sandbox_mode,
            &occurrence_turn,
            &[],
            profile_selected_audit,
        )
        .await
        .with_context(|| {
            format!(
                "failed to persist task run occurrence turn and permission audit `{}` for task `{}`",
                run.id, task.id
            )
        })?;
    processor
        .send_notification_to_thread_subscribers(
            parent_thread_id,
            events::TURN_STARTED,
            &TurnStartedNotification {
                workspace_id: task.workspace_id.clone(),
                thread_id: parent_thread_id.to_owned(),
                turn: occurrence_turn,
            },
        )
        .await;
    Ok(())
}

async fn ensure_task_run_occurrence_anchor(
    processor: &Arc<MessageProcessor>,
    task_response: &TaskGetResponse,
    parent_thread_id: &str,
    occurrence_turn_id: &str,
) -> Result<()> {
    let item = crate::task_tools::task_turn_item_from_response_for_run(
        processor,
        task_response,
        occurrence_turn_id,
        crate::task_tools::task_run_anchor_id(occurrence_turn_id),
    )
    .await?;
    if processor
        .crud_store
        .get_turn_item(occurrence_turn_id, item.id.as_str())
        .await?
        .is_some()
    {
        return Ok(());
    }

    let now = now_timestamp_secs();
    let item = pioneer_protocol::TurnItem::Task { item };
    let started = ItemStartedNotification {
        workspace_id: task_response.task.workspace_id.clone(),
        thread_id: parent_thread_id.to_owned(),
        turn_id: occurrence_turn_id.to_owned(),
        item: item.clone(),
    };
    processor
        .crud_store
        .materialize_item_started(started.clone(), now)
        .await
        .with_context(|| {
            format!("failed to persist task run occurrence anchor for turn `{occurrence_turn_id}`")
        })?;
    processor
        .send_notification_to_thread_subscribers(parent_thread_id, events::ITEM_STARTED, &started)
        .await;
    processor
        .notify_semantic_timeline_item_changed(
            started.workspace_id.as_str(),
            started.thread_id.as_str(),
            started.turn_id.as_str(),
            &started.item,
            Some("in_progress"),
        )
        .await;

    let completed = ItemCompletedNotification {
        workspace_id: task_response.task.workspace_id.clone(),
        thread_id: parent_thread_id.to_owned(),
        turn_id: occurrence_turn_id.to_owned(),
        item,
    };
    processor
        .crud_store
        .materialize_item_completed(completed.clone(), now)
        .await
        .with_context(|| {
            format!("failed to complete task run occurrence anchor for turn `{occurrence_turn_id}`")
        })?;
    processor
        .send_notification_to_thread_subscribers(
            parent_thread_id,
            events::ITEM_COMPLETED,
            &completed,
        )
        .await;
    processor
        .notify_semantic_timeline_item_changed(
            completed.workspace_id.as_str(),
            completed.thread_id.as_str(),
            completed.turn_id.as_str(),
            &completed.item,
            None,
        )
        .await;
    Ok(())
}

async fn mark_task_run_occurrence_turn_completed(
    processor: &Arc<MessageProcessor>,
    lineage: &TaskThreadLineage,
) -> Result<()> {
    mark_task_run_occurrence_turn_terminal(
        processor,
        lineage,
        TurnStatus::Completed,
        None,
        now_timestamp_secs(),
    )
    .await
}

async fn mark_task_run_occurrence_turn_failed(
    processor: &Arc<MessageProcessor>,
    lineage: &TaskThreadLineage,
    error_message: &str,
) -> Result<()> {
    mark_task_run_occurrence_turn_terminal(
        processor,
        lineage,
        TurnStatus::Failed,
        Some(error_message.to_owned()),
        now_timestamp_secs(),
    )
    .await
}

async fn mark_task_run_occurrence_turn_blocked(
    processor: &Arc<MessageProcessor>,
    lineage: &TaskThreadLineage,
    reason: &str,
) -> Result<()> {
    mark_task_run_occurrence_turn_terminal(
        processor,
        lineage,
        TurnStatus::Blocked,
        Some(reason.to_owned()),
        now_timestamp_secs(),
    )
    .await
}

async fn mark_task_run_occurrence_turn_terminal(
    processor: &Arc<MessageProcessor>,
    lineage: &TaskThreadLineage,
    status: TurnStatus,
    error: Option<String>,
    completed_at: i64,
) -> Result<()> {
    let Some(parent_turn_id) = lineage.created_by_turn_id.as_deref() else {
        return Ok(());
    };
    let parent_thread_id = lineage
        .created_by_thread_id
        .as_deref()
        .unwrap_or(lineage.parent_thread_id.as_str());
    let Some((workspace_id, mut turn)) = processor
        .crud_store
        .get_turn(parent_thread_id, parent_turn_id)
        .await?
    else {
        return Ok(());
    };
    if turn.turn_kind != TurnKind::TaskRun || turn.status != TurnStatus::InProgress {
        return Ok(());
    }
    turn.status = status;
    turn.error = error;
    match status {
        TurnStatus::Completed => {
            let notification = TurnCompletedNotification {
                workspace_id,
                thread_id: parent_thread_id.to_owned(),
                turn,
            };
            processor
                .crud_store
                .materialize_turn_completed(notification.clone(), completed_at)
                .await?;
            processor
                .send_notification_to_thread_subscribers(
                    parent_thread_id,
                    events::TURN_COMPLETED,
                    &notification,
                )
                .await;
            processor
                .notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
        }
        TurnStatus::Failed | TurnStatus::Interrupted => {
            let notification = TurnFailedNotification {
                workspace_id,
                thread_id: parent_thread_id.to_owned(),
                turn,
            };
            processor
                .crud_store
                .materialize_turn_failed(notification.clone(), completed_at)
                .await?;
            processor
                .send_notification_to_thread_subscribers(
                    parent_thread_id,
                    events::TURN_FAILED,
                    &notification,
                )
                .await;
            processor
                .notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
        }
        TurnStatus::Blocked => {
            let notification = TurnBlockedNotification {
                workspace_id,
                thread_id: parent_thread_id.to_owned(),
                turn,
                resume: None,
            };
            processor
                .crud_store
                .materialize_turn_blocked(notification.clone(), completed_at)
                .await?;
            processor
                .send_notification_to_thread_subscribers(
                    parent_thread_id,
                    events::TURN_BLOCKED,
                    &notification,
                )
                .await;
            processor
                .notify_semantic_timeline_turn_state_changed(
                    notification.workspace_id.as_str(),
                    notification.thread_id.as_str(),
                    notification.turn.id.as_str(),
                )
                .await;
        }
        TurnStatus::InProgress => {}
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct TaskRunChildRuntime {
    lineage: TaskThreadLineage,
    task_run_turn: TaskRunTurn,
}

async fn load_child_runtime_for_run(
    processor: &Arc<MessageProcessor>,
    run_id: &str,
) -> Result<Option<TaskRunChildRuntime>> {
    let Some(task_run_turn) = processor
        .crud_store
        .list_task_run_turns(run_id)
        .await?
        .into_iter()
        .rev()
        .find(|turn| turn.kind != TaskRunTurnKind::Review)
    else {
        return Ok(None);
    };
    load_child_runtime_from_task_run_turn(processor, task_run_turn)
        .await
        .map(Some)
}

async fn list_child_runtimes_for_run(
    processor: &Arc<MessageProcessor>,
    run_id: &str,
) -> Result<Vec<TaskRunChildRuntime>> {
    let task_run_turns = processor.crud_store.list_task_run_turns(run_id).await?;
    let mut runtimes = Vec::with_capacity(task_run_turns.len());
    for task_run_turn in task_run_turns {
        runtimes.push(load_child_runtime_from_task_run_turn(processor, task_run_turn).await?);
    }
    Ok(runtimes)
}

async fn load_child_runtime_for_turn(
    processor: &Arc<MessageProcessor>,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<TaskRunChildRuntime>> {
    if let Some(task_run_turn) = processor
        .crud_store
        .get_task_run_turn_by_turn(thread_id, turn_id)
        .await?
    {
        return load_child_runtime_from_task_run_turn(processor, task_run_turn)
            .await
            .map(Some);
    }
    Ok(None)
}

async fn load_child_runtime_from_task_run_turn(
    processor: &Arc<MessageProcessor>,
    task_run_turn: TaskRunTurn,
) -> Result<TaskRunChildRuntime> {
    if task_run_turn.kind != TaskRunTurnKind::Review
        && let Some(binding) = processor
            .crud_store
            .get_task_run_primary_thread_binding(task_run_turn.run_id.as_str())
            .await?
        && binding.thread_id != task_run_turn.thread_id
    {
        bail!(
            "primary task run thread binding `{}` points to `{}`, but task run turn `{}` points to `{}`",
            binding.id,
            binding.thread_id,
            task_run_turn.id,
            task_run_turn.thread_id
        );
    }

    let lineage = processor
        .crud_store
        .get_task_thread_lineage(task_run_turn.thread_id.as_str())
        .await?
        .unwrap_or_else(|| fallback_lineage_for_task_run_turn(&task_run_turn));
    Ok(TaskRunChildRuntime {
        lineage,
        task_run_turn,
    })
}

async fn load_execution_checkpoint_context_for_turn(
    processor: &Arc<MessageProcessor>,
    turn_id: &str,
) -> Result<Option<ExecutionCheckpointContext>> {
    let Some(checkpoint) = processor
        .crud_store
        .latest_turn_execution_checkpoint_for_turn(turn_id)
        .await?
    else {
        return Ok(None);
    };
    let Some(window) = processor
        .crud_store
        .get_turn_execution_window(checkpoint.window_id.as_str())
        .await?
    else {
        return Ok(None);
    };
    if window.turn_id != turn_id {
        warn!(
            turn_id,
            checkpoint_id = %checkpoint.id,
            window_turn_id = %window.turn_id,
            "skipping execution checkpoint whose window belongs to another turn"
        );
        return Ok(None);
    }
    let payload =
        match serde_json::from_value::<ExecutionCheckpointPayload>(checkpoint.payload_json.clone())
        {
            Ok(payload) => payload,
            Err(error) => {
                warn!(
                    turn_id,
                    checkpoint_id = %checkpoint.id,
                    error = %format!("{error:#}"),
                    "skipping invalid execution checkpoint payload during task child recovery"
                );
                return Ok(None);
            }
        };
    Ok(Some(ExecutionCheckpointContext {
        window_id: window.id,
        window_index: window.window_index,
        checkpoint_id: checkpoint.id,
        checkpoint_kind: task_execution_checkpoint_kind_label(checkpoint.checkpoint_kind),
        payload,
    }))
}

fn task_execution_checkpoint_kind_label(kind: pioneer_crud::TurnExecutionCheckpointKind) -> String {
    match kind {
        pioneer_crud::TurnExecutionCheckpointKind::WindowExhausted => "window_exhausted",
        pioneer_crud::TurnExecutionCheckpointKind::TurnBlocked => "turn_blocked",
        pioneer_crud::TurnExecutionCheckpointKind::StartupRecovery => "startup_recovery",
    }
    .to_owned()
}

fn task_run_primary_binding_from_turn(
    task: &Task,
    run: &TaskRun,
    execution: &TaskRunExecution,
    task_run_turn: &TaskRunTurn,
    created_at: i64,
) -> TaskRunThreadBinding {
    TaskRunThreadBinding {
        id: primary_task_run_thread_binding_id(run.id.as_str()),
        task_id: task.id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id.clone()),
        thread_id: task_run_turn.thread_id.clone(),
        binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
        created_at,
    }
}

fn initial_task_run_turn_from_execution(
    task: &Task,
    run: &TaskRun,
    execution: &TaskRunExecution,
    created_at: i64,
) -> TaskRunTurn {
    let child_thread_id = pioneer_protocol::generate_id(21);
    let child_turn_id = pioneer_protocol::generate_id(21);
    TaskRunTurn {
        id: task_run_turn_id_for_turn(child_turn_id.as_str()),
        task_id: task.id.clone(),
        run_id: run.id.clone(),
        execution_id: Some(execution.id.clone()),
        thread_id: child_thread_id,
        turn_id: child_turn_id,
        kind: TaskRunTurnKind::Initial,
        round: 0,
        sequence: 0,
        status: TaskRunTurnStatus::InProgress,
        reviews_candidate_id: None,
        requested_by_candidate_id: None,
        requested_by_review_event_id: None,
        created_at,
        started_at: Some(created_at),
        completed_at: None,
    }
}

fn lineage_from_task_run_turn(
    _task: &Task,
    _run: &TaskRun,
    agent_spec: &TaskAgentSpec,
    parent: &TaskParentRuntimeContext,
    task_run_turn: &TaskRunTurn,
    created_at: i64,
) -> TaskThreadLineage {
    TaskThreadLineage {
        child_thread_id: task_run_turn.thread_id.clone(),
        parent_thread_id: parent.parent_thread_id.clone(),
        root_thread_id: parent.root_thread_id.clone(),
        depth: agent_spec.depth,
        origin_kind: Some("task_run".to_owned()),
        created_by_thread_id: Some(parent.parent_thread_id.clone()),
        created_by_turn_id: parent.parent_turn_id.clone(),
        created_at,
    }
}

fn fallback_lineage_for_task_run_turn(task_run_turn: &TaskRunTurn) -> TaskThreadLineage {
    TaskThreadLineage {
        child_thread_id: task_run_turn.thread_id.clone(),
        parent_thread_id: task_run_turn.thread_id.clone(),
        root_thread_id: task_run_turn.thread_id.clone(),
        depth: 0,
        origin_kind: Some("task_run".to_owned()),
        created_by_thread_id: None,
        created_by_turn_id: None,
        created_at: task_run_turn.created_at,
    }
}

fn primary_task_run_thread_binding_id(run_id: &str) -> String {
    format!("trb_primary_{run_id}")
}

fn task_run_turn_id_for_turn(turn_id: &str) -> String {
    format!("trt_{turn_id}")
}

fn candidate_created_task_run_turn(task_run_turn: &TaskRunTurn, completed_at: i64) -> TaskRunTurn {
    let mut completed = task_run_turn.clone();
    completed.status = TaskRunTurnStatus::CandidateCreated;
    completed.completed_at = Some(completed_at);
    completed
}

fn failed_task_run_turn(
    task_run_turn: &TaskRunTurn,
    status: TaskRunTurnStatus,
    completed_at: i64,
) -> TaskRunTurn {
    let mut failed = task_run_turn.clone();
    failed.status = status;
    failed.completed_at = Some(completed_at);
    failed
}

fn blocked_task_run_turn(task_run_turn: &TaskRunTurn, completed_at: i64) -> TaskRunTurn {
    let mut blocked = task_run_turn.clone();
    blocked.status = TaskRunTurnStatus::Blocked;
    blocked.completed_at = Some(completed_at);
    blocked
}

fn revision_dispatch_error_details(task_run_turn: &TaskRunTurn) -> TaskValue {
    TaskValue::Object(BTreeMap::from([
        (
            "taskRunTurnId".to_owned(),
            TaskValue::String(task_run_turn.id.clone()),
        ),
        (
            "threadId".to_owned(),
            TaskValue::String(task_run_turn.thread_id.clone()),
        ),
        (
            "turnId".to_owned(),
            TaskValue::String(task_run_turn.turn_id.clone()),
        ),
        (
            "previousCandidateId".to_owned(),
            task_run_turn
                .requested_by_candidate_id
                .clone()
                .map(TaskValue::String)
                .unwrap_or(TaskValue::Null),
        ),
        (
            "requestedByReviewEventId".to_owned(),
            task_run_turn
                .requested_by_review_event_id
                .clone()
                .map(TaskValue::String)
                .unwrap_or(TaskValue::Null),
        ),
    ]))
}

async fn record_task_run_turn_failure(
    handle: &TaskExecutionHandle,
    task_run_turn: &TaskRunTurn,
    status: TaskRunTurnStatus,
    error: Option<TaskError>,
    completed_at: i64,
) -> Result<()> {
    handle
        .record_task_run_turn_failed(
            failed_task_run_turn(task_run_turn, status, completed_at),
            error,
            completed_at,
        )
        .await?;
    Ok(())
}

fn accepted_result_candidate(
    task_run_turn: &TaskRunTurn,
    result: TaskResult,
    accepted_at: i64,
) -> TaskResultCandidate {
    let review_event_id = runtime_auto_accept_review_event_id(
        task_run_turn.run_id.as_str(),
        task_run_turn.turn_id.as_str(),
    );
    TaskResultCandidate {
        id: task_result_candidate_id(
            task_run_turn.run_id.as_str(),
            task_run_turn.turn_id.as_str(),
        ),
        task_id: task_run_turn.task_id.clone(),
        run_id: task_run_turn.run_id.clone(),
        task_run_turn_id: task_run_turn.id.clone(),
        thread_id: task_run_turn.thread_id.clone(),
        turn_id: task_run_turn.turn_id.clone(),
        round: task_run_turn.round,
        status: TaskResultCandidateStatus::Accepted,
        summary: result.summary.clone(),
        result: Some(result),
        extraction_error: None,
        diagnostics: Vec::new(),
        final_review_event_id: Some(review_event_id),
        created_at: accepted_at,
        updated_at: accepted_at,
        resolved_at: Some(accepted_at),
    }
}

fn pending_review_result_candidate(
    task_run_turn: &TaskRunTurn,
    result: TaskResult,
    created_at: i64,
) -> TaskResultCandidate {
    TaskResultCandidate {
        id: task_result_candidate_id(
            task_run_turn.run_id.as_str(),
            task_run_turn.turn_id.as_str(),
        ),
        task_id: task_run_turn.task_id.clone(),
        run_id: task_run_turn.run_id.clone(),
        task_run_turn_id: task_run_turn.id.clone(),
        thread_id: task_run_turn.thread_id.clone(),
        turn_id: task_run_turn.turn_id.clone(),
        round: task_run_turn.round,
        status: TaskResultCandidateStatus::PendingReview,
        summary: result.summary.clone(),
        result: Some(result),
        extraction_error: None,
        diagnostics: Vec::new(),
        final_review_event_id: None,
        created_at,
        updated_at: created_at,
        resolved_at: None,
    }
}

fn extraction_failed_result_candidate(
    task_run_turn: &TaskRunTurn,
    error: TaskError,
    created_at: i64,
) -> TaskResultCandidate {
    let diagnostics = error
        .details
        .as_ref()
        .and_then(extraction_diagnostics_from_error_details)
        .unwrap_or_default();
    TaskResultCandidate {
        id: task_result_candidate_id(
            task_run_turn.run_id.as_str(),
            task_run_turn.turn_id.as_str(),
        ),
        task_id: task_run_turn.task_id.clone(),
        run_id: task_run_turn.run_id.clone(),
        task_run_turn_id: task_run_turn.id.clone(),
        thread_id: task_run_turn.thread_id.clone(),
        turn_id: task_run_turn.turn_id.clone(),
        round: task_run_turn.round,
        status: TaskResultCandidateStatus::ExtractionFailed,
        summary: Some(error.message.clone()),
        result: None,
        extraction_error: Some(error),
        diagnostics,
        final_review_event_id: None,
        created_at,
        updated_at: created_at,
        resolved_at: None,
    }
}

fn revision_possible(review_policy: &TaskAgentReviewPolicy, task_run_turn: &TaskRunTurn) -> bool {
    task_run_turn.round < review_policy.max_revision_rounds
}

fn invalid_structured_result_error(
    result: &TaskResult,
    agent_spec: &TaskAgentSpec,
    run_id: &str,
) -> Option<TaskError> {
    agent_spec.result_contract.as_ref()?;
    let TaskValue::Object(data) = result.data.as_ref()? else {
        return None;
    };
    let fallback_used = matches!(data.get("fallbackUsed"), Some(TaskValue::Bool(true)));
    let schema_invalid = matches!(data.get("schemaValid"), Some(TaskValue::Bool(false)));
    if !fallback_used && !schema_invalid {
        return None;
    }
    let diagnostics = data
        .get("diagnostics")
        .and_then(task_value_string_list)
        .unwrap_or_default();
    let message = if diagnostics.is_empty() {
        "child task result did not satisfy the result contract".to_owned()
    } else {
        format!(
            "child task result did not satisfy the result contract: {}",
            diagnostics.join("; ")
        )
    };
    Some(TaskError {
        code: "task_agent_result_extraction_failed".to_owned(),
        message,
        class: TaskErrorClass::Validation,
        details: Some(TaskValue::Object(BTreeMap::from([
            ("schemaValid".to_owned(), TaskValue::Bool(!schema_invalid)),
            ("fallbackUsed".to_owned(), TaskValue::Bool(fallback_used)),
            (
                "diagnostics".to_owned(),
                TaskValue::List(diagnostics.into_iter().map(TaskValue::String).collect()),
            ),
        ]))),
        failed_run_id: Some(run_id.to_owned()),
    })
}

fn task_value_string_list(value: &TaskValue) -> Option<Vec<String>> {
    let TaskValue::List(items) = value else {
        return None;
    };
    Some(
        items
            .iter()
            .filter_map(|item| match item {
                TaskValue::String(value) => Some(value.clone()),
                _ => None,
            })
            .collect(),
    )
}

fn extraction_diagnostics_from_error_details(details: &TaskValue) -> Option<Vec<String>> {
    let TaskValue::Object(object) = details else {
        return None;
    };
    object.get("diagnostics").and_then(task_value_string_list)
}

fn runtime_auto_accept_review_event(
    candidate: &TaskResultCandidate,
    accepted_at: i64,
) -> TaskResultReviewEvent {
    TaskResultReviewEvent {
        id: candidate.final_review_event_id.clone().unwrap_or_else(|| {
            runtime_auto_accept_review_event_id(
                candidate.run_id.as_str(),
                candidate.turn_id.as_str(),
            )
        }),
        candidate_id: candidate.id.clone(),
        task_id: candidate.task_id.clone(),
        run_id: candidate.run_id.clone(),
        task_run_turn_id: candidate.task_run_turn_id.clone(),
        reviewer_kind: TaskResultReviewerKind::RuntimeAuto,
        reviewer_thread_id: None,
        reviewer_turn_id: None,
        reviewer_user_id: None,
        reviewer_agent_spec_id: None,
        event_kind: TaskResultReviewEventKind::SystemAuto,
        decision: TaskResultReviewDecision::Accept,
        feedback_text: None,
        feedback: None,
        confidence: None,
        supersedes_review_event_id: None,
        next_task_run_turn_id: None,
        created_at: accepted_at,
    }
}

fn task_result_candidate_id(run_id: &str, turn_id: &str) -> String {
    format!("trc_{run_id}_{turn_id}")
}

fn runtime_auto_accept_review_event_id(run_id: &str, turn_id: &str) -> String {
    format!("trre_auto_{run_id}_{turn_id}")
}

fn task_run_turn_terminal_status_from_child_turn_status(
    status: TurnStatus,
) -> Option<TaskRunTurnStatus> {
    match status {
        TurnStatus::Failed => Some(TaskRunTurnStatus::Failed),
        TurnStatus::Interrupted => Some(TaskRunTurnStatus::Interrupted),
        TurnStatus::Blocked => Some(TaskRunTurnStatus::Blocked),
        TurnStatus::Completed | TurnStatus::InProgress => None,
    }
}

fn spawn_execution_heartbeat(
    processor: &Arc<MessageProcessor>,
    execution_id: String,
    child_thread_id: String,
    child_turn_id: String,
    run_id: String,
) {
    let processor = processor.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(TASK_EXECUTION_HEARTBEAT_SECONDS)).await;
            let Ok(Some(execution)) = processor.crud_store.load_execution_for_run(&run_id).await
            else {
                break;
            };
            if execution.id != execution_id || execution.status.is_terminal() {
                break;
            }
            let Ok(turn) = processor
                .crud_store
                .get_turn(child_thread_id.as_str(), child_turn_id.as_str())
                .await
            else {
                break;
            };
            let Some((_, turn)) = turn else {
                break;
            };
            if turn.status != TurnStatus::InProgress {
                break;
            }
            let now = now_timestamp_secs();
            let _ = processor
                .crud_store
                .heartbeat_execution(
                    execution_id.as_str(),
                    now,
                    Some(now.saturating_add(TASK_EXECUTION_LEASE_SECONDS)),
                )
                .await;
        }
    });
}

fn effective_agent_model(agent_spec: &TaskAgentSpec) -> Result<EffectiveAgentModel> {
    let model = agent_spec
        .model
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("task agent spec `{}` is missing `model`", agent_spec.id))?;
    let model_provider = agent_spec
        .model_provider
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "task agent spec `{}` is missing `model_provider`",
                agent_spec.id
            )
        })?;

    Ok(EffectiveAgentModel {
        model: model.to_owned(),
        model_provider: model_provider.to_owned(),
    })
}

fn effective_task_child_permission_profile(
    agent_spec: &TaskAgentSpec,
    launch_profile: Option<&TurnPermissionProfileSnapshot>,
) -> Result<TurnPermissionProfileSnapshot> {
    let cap = agent_spec.permission_cap.as_ref().ok_or_else(|| {
        anyhow!(
            "task agent spec `{}` is missing permission_cap",
            agent_spec.id
        )
    })?;
    let cap_profile = pioneer_protocol::task_permission_cap_snapshot(cap);
    let launcher = launch_profile
        .cloned()
        .unwrap_or_else(pioneer_protocol::default_turn_permission_profile_snapshot);
    let mut profile = pioneer_protocol::intersect_turn_permission_profiles(
        &cap_profile,
        &launcher,
        TurnPermissionProfileSource::TaskPermissionCap,
    );
    if let Some(tool_policy) = agent_spec.tool_policy.as_ref() {
        apply_task_tool_policy_to_permission_profile(&mut profile, tool_policy);
    }
    Ok(profile)
}

fn apply_task_tool_policy_to_permission_profile(
    profile: &mut TurnPermissionProfileSnapshot,
    tool_policy: &TaskAgentToolPolicy,
) {
    let task_policy = task_tool_policy_permission_snapshot(tool_policy);
    profile.effective_policy = pioneer_protocol::intersect_tool_permission_policies(
        &profile.effective_policy,
        &task_policy,
    );
}

fn task_tool_policy_permission_snapshot(
    tool_policy: &TaskAgentToolPolicy,
) -> ToolPermissionPolicySnapshot {
    let mut policy = ToolPermissionPolicySnapshot::all(PermissionBehavior::Allow);
    match tool_policy.write_mode {
        TaskAgentWriteMode::ReadOnly => {
            policy.file_write = PermissionBehavior::Deny;
        }
        TaskAgentWriteMode::WorkspaceWrite | TaskAgentWriteMode::ScopedWrite => {}
        TaskAgentWriteMode::FullAccess => {}
    }
    if !tool_policy.network_access {
        policy.network = PermissionBehavior::Deny;
    }
    policy.allowed_tools = normalized_task_policy_values(&tool_policy.allowed_tools);
    policy.denied_tools = normalized_task_policy_values(&tool_policy.denied_tools);
    policy.allowed_paths = normalized_task_policy_values(&tool_policy.allowed_paths);
    policy
}

fn normalized_task_policy_values(values: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !normalized
            .iter()
            .any(|existing: &String| existing == trimmed)
        {
            normalized.push(trimmed.to_owned());
        }
    }
    normalized
}

fn select_agent_spec(response: &TaskGetResponse, run_id: &str) -> Option<TaskAgentSpec> {
    response
        .agent_specs
        .iter()
        .rev()
        .find(|spec| spec.run_id.as_deref() == Some(run_id))
        .or_else(|| {
            response
                .agent_specs
                .iter()
                .rev()
                .find(|spec| spec.run_id.is_none())
        })
        .cloned()
}

#[derive(Debug, Clone)]
struct RevisionPromptContext {
    task_run_turn: TaskRunTurn,
    previous_candidate: TaskResultCandidate,
    review_event: TaskResultReviewEvent,
    additional_instructions: Vec<String>,
}

async fn load_revision_prompt_context(
    processor: &Arc<MessageProcessor>,
    task_run_turn: &TaskRunTurn,
) -> Result<RevisionPromptContext> {
    let candidate_id = task_run_turn
        .requested_by_candidate_id
        .as_deref()
        .ok_or_else(|| {
            anyhow!(
                "revision task run turn `{}` has no requested_by_candidate_id",
                task_run_turn.id
            )
        })?;
    let previous_candidate = processor
        .crud_store
        .get_task_result_candidate(candidate_id)
        .await?
        .ok_or_else(|| anyhow!("revision candidate `{candidate_id}` not found"))?;
    let review_event = match task_run_turn.requested_by_review_event_id.as_deref() {
        Some(review_event_id) => processor
            .crud_store
            .get_task_result_review_event(review_event_id)
            .await?
            .ok_or_else(|| anyhow!("revision review event `{review_event_id}` not found"))?,
        None => processor
            .crud_store
            .list_task_result_review_events(candidate_id)
            .await?
            .into_iter()
            .find(|event| event.next_task_run_turn_id.as_deref() == Some(task_run_turn.id.as_str()))
            .ok_or_else(|| {
                anyhow!(
                    "revision task run turn `{}` has no matching review event",
                    task_run_turn.id
                )
            })?,
    };
    Ok(RevisionPromptContext {
        task_run_turn: task_run_turn.clone(),
        additional_instructions: revision_additional_instructions_from_feedback(
            review_event.feedback.as_ref(),
        ),
        previous_candidate,
        review_event,
    })
}

fn revision_additional_instructions_from_feedback(feedback: Option<&TaskValue>) -> Vec<String> {
    let Some(TaskValue::Object(object)) = feedback else {
        return Vec::new();
    };
    let Some(TaskValue::List(values)) = object.get("additionalInstructions") else {
        return Vec::new();
    };
    values
        .iter()
        .filter_map(|value| match value {
            TaskValue::String(value) => Some(value.clone()),
            _ => None,
        })
        .collect()
}

async fn task_revise_response_from_store(
    processor: &Arc<MessageProcessor>,
    response: TaskReviseResponse,
) -> Result<TaskReviseResponse> {
    let task_response = processor
        .crud_store
        .get_task(response.task.id.as_str())
        .await?
        .ok_or_else(|| anyhow!("task `{}` not found", response.task.id))?;
    let run = task_response
        .runs
        .iter()
        .find(|run| run.id == response.run.id)
        .cloned()
        .ok_or_else(|| anyhow!("task run `{}` not found", response.run.id))?;
    let candidate = processor
        .crud_store
        .get_task_result_candidate(response.candidate.id.as_str())
        .await?
        .unwrap_or(response.candidate);
    let review_event = processor
        .crud_store
        .get_task_result_review_event(response.review_event.id.as_str())
        .await?
        .unwrap_or(response.review_event);
    let task_run_turn = processor
        .crud_store
        .get_task_run_turn(response.task_run_turn.id.as_str())
        .await?
        .unwrap_or(response.task_run_turn);
    Ok(TaskReviseResponse {
        status: task_response.task.status,
        task: task_response.task,
        run,
        candidate,
        review_event,
        child_thread_id: task_run_turn.thread_id.clone(),
        child_turn_id: task_run_turn.turn_id.clone(),
        round: task_run_turn.round,
        task_run_turn,
        requested: response.requested,
        already_requested: response.already_requested,
        feedback: response.feedback,
        additional_instructions: response.additional_instructions,
    })
}

async fn materialize_child_task_prompt(
    processor: &Arc<MessageProcessor>,
    task_response: &TaskGetResponse,
    run: &TaskRun,
    agent_spec: &TaskAgentSpec,
    parent: &TaskParentRuntimeContext,
    task_run_turn: Option<&TaskRunTurn>,
    effective_permission_profile: &TurnPermissionProfileSnapshot,
) -> Result<String> {
    let parent_context = render_context_policy(
        processor,
        task_response.task.workspace_id.as_str(),
        agent_spec,
        parent,
    )
    .await?;
    let trigger = run
        .trigger_id
        .as_deref()
        .and_then(|trigger_id| find_task_run_trigger(task_response, trigger_id));
    let revision_context = match task_run_turn {
        Some(task_run_turn) if task_run_turn.kind == TaskRunTurnKind::Revision => {
            Some(load_revision_prompt_context(processor, task_run_turn).await?)
        }
        _ => None,
    };
    let revision = revision_context
        .as_ref()
        .map(|context| TaskRevisionPromptInput {
            task_run_turn: &context.task_run_turn,
            previous_candidate: &context.previous_candidate,
            review_event: &context.review_event,
            additional_instructions: &context.additional_instructions,
        });
    Ok(TaskRunPromptCompiler::new().compile(TaskRunPromptInput {
        task: &task_response.task,
        run,
        trigger,
        agent_spec,
        now: now_timestamp_secs(),
        parent_context: parent_context.as_deref(),
        output_instructions: agent_spec.prompt.output_instructions.as_deref(),
        effective_permission_profile,
        revision,
    }))
}

fn materialize_child_task_input(prompt: String, agent_spec: &TaskAgentSpec) -> Vec<UserInput> {
    let mut input = vec![UserInput::Text {
        text: prompt,
        text_elements: Vec::new(),
    }];
    input.extend(task_artifacts::task_agent_artifact_user_inputs(agent_spec));
    input
}

fn materialize_reviewer_prompt(
    task_response: &TaskGetResponse,
    agent_spec: &TaskAgentSpec,
    review_policy: &TaskAgentReviewPolicy,
    candidate: &TaskResultCandidate,
    reviewer_spec: &TaskResultReviewerSpec,
    reviewer_key: &str,
) -> String {
    let result_json = candidate
        .result
        .as_ref()
        .and_then(|result| serde_json::to_string_pretty(result).ok())
        .unwrap_or_else(|| "null".to_owned());
    let extraction_error_json = candidate
        .extraction_error
        .as_ref()
        .and_then(|error| serde_json::to_string_pretty(error).ok())
        .unwrap_or_else(|| "null".to_owned());
    let reviewer_role = reviewer_spec
        .agent_role
        .as_deref()
        .or(reviewer_spec.agent_nickname.as_deref())
        .unwrap_or("reviewer");
    format!(
        r#"You are reviewing a child agent result for a task.

Task title:
{title}

Task goal:
{goal}

Original child-agent instructions:
{instructions}

Review policy:
- strategy: {strategy:?}
- max revision rounds: {max_revision_rounds}
- reviewer key: {reviewer_key}
- reviewer role: {reviewer_role}
- required reviewer: {required}

Candidate:
- id: {candidate_id}
- round: {round}
- status: {status:?}
- summary: {summary}

Candidate result JSON:
{result_json}

Candidate extraction error JSON:
{extraction_error_json}

Return only one JSON object:
{{
  "decision": "accept" | "request_changes" | "reject" | "abstain",
  "feedback": "short actionable feedback",
  "confidence": 0.0
}}
"#,
        title = task_response.task.title,
        goal = task_response.task.goal,
        instructions = agent_spec.prompt.instructions.join("\n"),
        strategy = review_policy.resolution_strategy,
        max_revision_rounds = review_policy.max_revision_rounds,
        required = reviewer_spec.required,
        candidate_id = candidate.id,
        round = candidate.round,
        status = candidate.status,
        summary = candidate.summary.as_deref().unwrap_or(""),
    )
}

#[derive(Debug, Clone, PartialEq)]
struct ReviewerAdvisory {
    decision: TaskResultReviewDecision,
    feedback_text: Option<String>,
    feedback: Option<TaskValue>,
    confidence: Option<f64>,
}

async fn extract_reviewer_advisory(
    processor: &Arc<MessageProcessor>,
    turn_id: &str,
) -> Result<ReviewerAdvisory> {
    let messages = processor
        .crud_store
        .list_completed_agent_messages(turn_id)
        .await?;
    let final_text = messages.into_iter().rev().find_map(|item| match item {
        TurnItem::AgentMessage { text, .. } => Some(text),
        _ => None,
    });
    Ok(match final_text {
        Some(text) => parse_reviewer_advisory_text(text.as_str()),
        None => ReviewerAdvisory {
            decision: TaskResultReviewDecision::Abstain,
            feedback_text: Some("reviewer turn completed without a final agent message".to_owned()),
            feedback: None,
            confidence: None,
        },
    })
}

fn parse_reviewer_advisory_text(raw: &str) -> ReviewerAdvisory {
    let parsed = serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .or_else(|| extract_json_object(raw).and_then(|json| serde_json::from_str(&json).ok()));
    let Some(value) = parsed else {
        return ReviewerAdvisory {
            decision: fallback_review_decision(raw),
            feedback_text: Some(raw.trim().to_owned()).filter(|text| !text.is_empty()),
            feedback: None,
            confidence: None,
        };
    };
    let decision = value
        .get("decision")
        .and_then(|value| value.as_str())
        .and_then(parse_review_decision)
        .unwrap_or(TaskResultReviewDecision::Abstain);
    let feedback_text = value
        .get("feedback")
        .or_else(|| value.get("feedbackText"))
        .or_else(|| value.get("reason"))
        .and_then(|value| value.as_str())
        .map(str::to_owned);
    let confidence = value.get("confidence").and_then(|value| value.as_f64());
    ReviewerAdvisory {
        decision,
        feedback_text,
        feedback: Some(task_value_from_json(value)),
        confidence,
    }
}

fn extract_json_object(raw: &str) -> Option<String> {
    let start = raw.find('{')?;
    let end = raw.rfind('}')?;
    (end >= start).then(|| raw[start..=end].to_owned())
}

fn parse_review_decision(value: &str) -> Option<TaskResultReviewDecision> {
    match value.trim().to_ascii_lowercase().as_str() {
        "accept" | "accepted" | "approve" | "approved" => Some(TaskResultReviewDecision::Accept),
        "request_changes" | "request changes" | "revise" | "needs_changes" => {
            Some(TaskResultReviewDecision::RequestChanges)
        }
        "reject" | "rejected" => Some(TaskResultReviewDecision::Reject),
        "cancel" | "cancelled" | "canceled" => Some(TaskResultReviewDecision::Cancel),
        "abstain" | "unknown" => Some(TaskResultReviewDecision::Abstain),
        _ => None,
    }
}

fn fallback_review_decision(raw: &str) -> TaskResultReviewDecision {
    let lower = raw.to_ascii_lowercase();
    if lower.contains("request changes")
        || lower.contains("needs changes")
        || lower.contains("revise")
    {
        TaskResultReviewDecision::RequestChanges
    } else if lower.contains("reject") {
        TaskResultReviewDecision::Reject
    } else if lower.contains("accept") || lower.contains("approve") {
        TaskResultReviewDecision::Accept
    } else {
        TaskResultReviewDecision::Abstain
    }
}

fn reviewer_key_for_turn(
    review_policy: Option<&TaskAgentReviewPolicy>,
    candidate: &TaskResultCandidate,
    task_run_turn: &TaskRunTurn,
) -> Option<String> {
    let review_policy = review_policy?;
    review_policy
        .reviewers
        .iter()
        .enumerate()
        .map(|(index, spec)| task_result_reviewer_spec_key(index, spec))
        .find(|key| {
            stable_review_thread_id(candidate.id.as_str(), key.as_str()) == task_run_turn.thread_id
        })
}

fn reviewer_thread_name(task: &Task, reviewer_spec: &TaskResultReviewerSpec) -> String {
    let reviewer = reviewer_spec
        .agent_nickname
        .as_deref()
        .or(reviewer_spec.agent_role.as_deref())
        .unwrap_or("Reviewer");
    format!("{reviewer}: {}", task.title)
}

fn find_task_run_trigger<'a>(
    task_response: &'a TaskGetResponse,
    trigger_id: &str,
) -> Option<&'a TaskTrigger> {
    task_response
        .triggers
        .iter()
        .find(|trigger| trigger.id == trigger_id)
}

fn thread_name_from_task(task: &Task) -> Option<String> {
    let trimmed = task.title.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

async fn render_context_policy(
    processor: &Arc<MessageProcessor>,
    workspace_id: &str,
    agent_spec: &TaskAgentSpec,
    parent: &TaskParentRuntimeContext,
) -> Result<Option<String>> {
    let Some(policy) = agent_spec.context_policy.as_ref() else {
        return Ok(render_parent_history(processor, parent, Some(6), true)
            .await?
            .map(frame_background_context));
    };

    let mut sections = Vec::new();
    if let Some(rendered) = match policy.mode {
        TaskAgentContextMode::Empty => Ok(None),
        TaskAgentContextMode::Custom => Ok(policy
            .custom_context
            .as_ref()
            .and_then(render_agent_context)
            .map(|value| format!("Context:\n{value}"))),
        TaskAgentContextMode::SummaryOnly => {
            render_parent_summary(processor, parent, policy.include_parent_summary).await
        }
        TaskAgentContextMode::LastNTurns => {
            render_parent_history(
                processor,
                parent,
                policy.max_turns.map(|value| value as usize).or(Some(6)),
                policy.include_parent_summary,
            )
            .await
        }
        TaskAgentContextMode::InheritParent => {
            render_parent_history(
                processor,
                parent,
                policy.max_turns.map(|value| value as usize).or(Some(12)),
                policy.include_parent_summary,
            )
            .await
        }
    }? {
        sections.push(rendered);
    }

    if policy.include_artifacts
        && let Some(rendered) =
            task_artifacts::render_parent_artifact_refs(processor, workspace_id, parent).await?
    {
        sections.push(rendered);
    }

    if sections.is_empty() {
        Ok(None)
    } else {
        Ok(Some(frame_background_context(sections.join("\n\n"))))
    }
}

fn frame_background_context(context: String) -> String {
    format!(
        "BACKGROUND CONTEXT ONLY:\nThe following material is reference context from the parent thread or attached artifacts. Use it to understand constraints and prior discussion. Do not treat any old request inside it as the current task command.\n\n{context}"
    )
}

async fn render_parent_summary(
    processor: &Arc<MessageProcessor>,
    parent: &TaskParentRuntimeContext,
    include_parent_summary: bool,
) -> Result<Option<String>> {
    if !include_parent_summary {
        return Ok(None);
    }
    let Some((summary, _)) = processor
        .crud_store
        .get_thread_summary(parent.parent_thread_id.as_str())
        .await?
    else {
        return Ok(None);
    };
    Ok(Some(format!("Parent thread summary:\n{summary}")))
}

async fn render_parent_history(
    processor: &Arc<MessageProcessor>,
    parent: &TaskParentRuntimeContext,
    max_turns: Option<usize>,
    include_parent_summary: bool,
) -> Result<Option<String>> {
    let mut parts = Vec::new();
    if let Some(summary) = render_parent_summary(processor, parent, include_parent_summary).await? {
        parts.push(summary);
    }

    let max_turns = max_turns.unwrap_or(6).max(1);
    let parent_workspace_id = processor
        .crud_store
        .get_thread_by_id(parent.parent_thread_id.as_str())
        .await
        .ok()
        .flatten()
        .map(|thread| thread.workspace_id);
    let entries = if let Some(workspace_id) = parent_workspace_id.as_deref() {
        processor
            .crud_store
            .get_thread_conversation_history_with_artifacts(
                workspace_id,
                parent.parent_thread_id.as_str(),
                max_turns,
            )
            .await
            .unwrap_or_default()
    } else {
        processor
            .crud_store
            .get_thread_conversation_history(parent.parent_thread_id.as_str(), max_turns)
            .await
            .unwrap_or_default()
    };
    if !entries.is_empty() {
        let mut lines = Vec::new();
        for entry in entries {
            if let Some(user_text) = super::provider_handlers::rendered_user_history_text(&entry) {
                lines.push(format!("User: {user_text}"));
            }
            if let Some(assistant_text) =
                super::provider_handlers::rendered_assistant_history_text(&entry)
            {
                lines.push(format!("Assistant: {assistant_text}"));
            }
        }
        if !lines.is_empty() {
            parts.push(format!(
                "Recent parent thread context:\n{}",
                lines.join("\n")
            ));
        }
    }

    if parts.is_empty() {
        Ok(None)
    } else {
        Ok(Some(parts.join("\n\n")))
    }
}

fn render_agent_input(input: &TaskAgentInput) -> Option<String> {
    let mut lines = Vec::new();
    if let Some(text) = input.text.as_deref()
        && !text.trim().is_empty()
    {
        lines.push(text.to_owned());
    }
    for variable in &input.variables {
        let value = serde_json::to_string(&variable.value)
            .unwrap_or_else(|_| "<unserializable>".to_owned());
        lines.push(format!("Variable {}: {}", variable.name, value));
    }
    for attachment in &input.attachments {
        lines.push(format!(
            "Attachment {:?}: {}",
            attachment.kind,
            attachment
                .name
                .as_deref()
                .or(attachment.path.as_deref())
                .or(attachment.url.as_deref())
                .or(attachment.artifact_id.as_deref())
                .unwrap_or("unnamed")
        ));
    }
    for reference in &input.references {
        lines.push(format!(
            "Reference {:?}: {}{}",
            reference.kind,
            reference.id,
            reference
                .label
                .as_ref()
                .map(|label| format!(" ({label})"))
                .unwrap_or_default()
        ));
    }
    (!lines.is_empty()).then(|| lines.join("\n"))
}

fn render_agent_context(context: &TaskAgentContext) -> Option<String> {
    let input = TaskAgentInput {
        text: context.text.clone(),
        variables: context.variables.clone(),
        attachments: context.attachments.clone(),
        references: context.references.clone(),
    };
    render_agent_input(&input)
}

struct TaskAgentResultExtractor;

type TaskAgentResultExtraction = std::result::Result<TaskResult, TaskError>;

enum TaskAgentResultArtifactMode {
    FinalResult,
    ResultCandidate { candidate_id: String },
}

struct StructuredResultCandidate {
    value: TaskValue,
}

impl TaskAgentResultExtractor {
    async fn extract_with_artifact_mode(
        processor: &Arc<MessageProcessor>,
        task_run_turn: &TaskRunTurn,
        lineage: &TaskThreadLineage,
        artifact_mode: TaskAgentResultArtifactMode,
    ) -> Result<TaskAgentResultExtraction> {
        let task_response = match processor
            .crud_store
            .get_task(task_run_turn.task_id.as_str())
            .await?
        {
            Some(response) => response,
            None => {
                return Ok(Err(task_error(
                    "task_missing",
                    format!(
                        "task `{}` was not found for result extraction",
                        task_run_turn.task_id
                    ),
                    TaskErrorClass::Internal,
                    Some(task_run_turn.run_id.clone()),
                )));
            }
        };
        let contract = select_agent_spec(&task_response, task_run_turn.run_id.as_str())
            .and_then(|spec| spec.result_contract);

        let messages = processor
            .crud_store
            .list_completed_agent_messages(task_run_turn.turn_id.as_str())
            .await?;
        let final_message = messages.into_iter().rev().find_map(|item| match item {
            TurnItem::AgentMessage { id, text, .. } => Some((id, text)),
            _ => None,
        });
        let Some((source_item_id, raw_text)) = final_message else {
            return Ok(Err(task_error(
                "task_agent_result_missing",
                "child task turn completed without a final agent message".to_owned(),
                TaskErrorClass::Validation,
                Some(task_run_turn.run_id.clone()),
            )));
        };

        match Self::normalize_final_message(
            raw_text,
            source_item_id,
            task_run_turn,
            contract.as_ref(),
        ) {
            Ok(result) => {
                Self::normalize_result_artifacts(
                    processor,
                    &task_response,
                    task_run_turn,
                    lineage,
                    result,
                    artifact_mode,
                )
                .await
            }
            Err(error) => Ok(Err(error)),
        }
    }

    async fn normalize_result_artifacts(
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        task_run_turn: &TaskRunTurn,
        lineage: &TaskThreadLineage,
        result: TaskResult,
        artifact_mode: TaskAgentResultArtifactMode,
    ) -> Result<TaskAgentResultExtraction> {
        match artifact_mode {
            TaskAgentResultArtifactMode::FinalResult => {
                task_artifacts::normalize_task_result_artifacts(
                    processor,
                    &task_response.task,
                    task_run_turn,
                    lineage,
                    result,
                )
                .await
            }
            TaskAgentResultArtifactMode::ResultCandidate { candidate_id } => {
                task_artifacts::normalize_task_result_candidate_artifacts(
                    processor,
                    &task_response.task,
                    task_run_turn,
                    lineage,
                    candidate_id.as_str(),
                    result,
                )
                .await
            }
        }
    }

    fn normalize_final_message(
        raw_text: String,
        source_item_id: String,
        task_run_turn: &TaskRunTurn,
        contract: Option<&TaskAgentResultContract>,
    ) -> TaskAgentResultExtraction {
        let mut diagnostics = Vec::new();
        if let Some(candidate) =
            extract_structured_result_candidate(raw_text.as_str(), contract, &mut diagnostics)
        {
            let schema_errors = validate_structured_candidate(&candidate.value, contract);
            if schema_errors.is_empty() {
                return Ok(task_result_from_structured_candidate(
                    candidate,
                    raw_text.as_str(),
                    task_run_turn,
                ));
            }
            diagnostics.extend(schema_errors);
        }

        Ok(fallback_text_task_result(
            raw_text.as_str(),
            source_item_id,
            task_run_turn,
            diagnostics,
        ))
    }
}

fn task_result_from_structured_candidate(
    candidate: StructuredResultCandidate,
    raw_text: &str,
    task_run_turn: &TaskRunTurn,
) -> TaskResult {
    let (summary, data, artifacts) = match candidate.value {
        TaskValue::Object(mut object) => {
            let artifacts = object
                .remove("artifacts")
                .and_then(|value| match value {
                    TaskValue::List(values) => Some(task_artifacts::parse_task_artifacts(&values)),
                    _ => None,
                })
                .unwrap_or_default();
            let summary = object
                .remove("summary")
                .and_then(|value| task_value_into_string(value))
                .or_else(|| first_meaningful_line(raw_text));
            let data = object
                .remove("data")
                .or_else(|| (!object.is_empty()).then(|| TaskValue::Object(object)));
            (summary, data, artifacts)
        }
        TaskValue::String(value) => (
            Some(value.clone()),
            Some(TaskValue::String(value)),
            Vec::new(),
        ),
        value => (first_meaningful_line(raw_text), Some(value), Vec::new()),
    };

    let mut result = TaskResult {
        summary,
        data,
        artifacts,
        completed_by_run_id: Some(task_run_turn.run_id.clone()),
    };
    if let Some(TaskValue::Object(object)) = &result.data
        && let Some(TaskValue::List(artifact_values)) = object.get("artifacts")
    {
        result.artifacts = task_artifacts::parse_task_artifacts(artifact_values);
    }
    result
}

fn fallback_text_task_result(
    raw_text: &str,
    source_item_id: String,
    task_run_turn: &TaskRunTurn,
    diagnostics: Vec<String>,
) -> TaskResult {
    let plain_text = strip_structured_result_blocks(raw_text);
    let fallback_text = if plain_text.trim().is_empty() {
        raw_text.to_owned()
    } else {
        plain_text
    };
    let data = TaskValue::Object(BTreeMap::from([
        (
            "rawText".to_owned(),
            TaskValue::String(fallback_text.clone()),
        ),
        ("schemaValid".to_owned(), TaskValue::Bool(false)),
        ("fallbackUsed".to_owned(), TaskValue::Bool(true)),
        (
            "diagnostics".to_owned(),
            TaskValue::List(diagnostics.into_iter().map(TaskValue::String).collect()),
        ),
        (
            "sourceThreadId".to_owned(),
            TaskValue::String(task_run_turn.thread_id.clone()),
        ),
        (
            "sourceTurnId".to_owned(),
            TaskValue::String(task_run_turn.turn_id.clone()),
        ),
        ("sourceItemId".to_owned(), TaskValue::String(source_item_id)),
    ]));
    TaskResult {
        summary: first_meaningful_line(fallback_text.as_str()),
        data: Some(data),
        artifacts: Vec::new(),
        completed_by_run_id: Some(task_run_turn.run_id.clone()),
    }
}

fn extract_structured_result_candidate(
    raw_text: &str,
    contract: Option<&TaskAgentResultContract>,
    diagnostics: &mut Vec<String>,
) -> Option<StructuredResultCandidate> {
    for (source, snippet) in collect_structured_result_snippets(raw_text, contract) {
        match serde_json::from_str::<serde_json::Value>(snippet.trim()) {
            Ok(value) => {
                return Some(StructuredResultCandidate {
                    value: task_value_from_json(value),
                });
            }
            Err(error) => diagnostics.push(format!("{source} parse failed: {error}")),
        }
    }
    None
}

fn collect_structured_result_snippets(
    raw_text: &str,
    contract: Option<&TaskAgentResultContract>,
) -> Vec<(String, String)> {
    let mut snippets = Vec::new();
    collect_tagged_snippets(
        raw_text,
        "<task_result>",
        "</task_result>",
        "task_result tag",
        &mut snippets,
    );
    collect_tagged_snippets(
        raw_text,
        "<task-result>",
        "</task-result>",
        "task-result tag",
        &mut snippets,
    );

    let (task_fences, json_fences) = collect_fenced_result_snippets(raw_text, contract);
    snippets.extend(task_fences);
    snippets.extend(json_fences);

    let trimmed = raw_text.trim();
    if matches!(
        contract.map(|contract| contract.format),
        Some(TaskAgentResultFormat::Json)
    ) && (trimmed.starts_with('{') || trimmed.starts_with('['))
    {
        snippets.push(("whole json message".to_owned(), trimmed.to_owned()));
    }
    snippets
}

fn collect_tagged_snippets(
    raw_text: &str,
    open_tag: &str,
    close_tag: &str,
    source: &str,
    snippets: &mut Vec<(String, String)>,
) {
    let lower = raw_text.to_ascii_lowercase();
    let mut cursor = 0;
    while let Some(open_offset) = lower[cursor..].find(open_tag) {
        let start = cursor + open_offset + open_tag.len();
        let Some(close_offset) = lower[start..].find(close_tag) else {
            break;
        };
        let end = start + close_offset;
        snippets.push((source.to_owned(), raw_text[start..end].to_owned()));
        cursor = end + close_tag.len();
    }
}

fn collect_fenced_result_snippets(
    raw_text: &str,
    contract: Option<&TaskAgentResultContract>,
) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let mut task_fences = Vec::new();
    let mut json_fences = Vec::new();
    let mut active_info: Option<String> = None;
    let mut body = Vec::new();

    for line in raw_text.lines() {
        let trimmed = line.trim_start();
        if let Some(info) = trimmed.strip_prefix("```") {
            if let Some(open_info) = active_info.take() {
                let snippet = body.join("\n");
                if is_task_result_fence(open_info.as_str()) {
                    task_fences.push((format!("fenced {open_info} block"), snippet));
                } else if is_json_result_fence(open_info.as_str(), contract) {
                    json_fences.push((format!("fenced {open_info} block"), snippet));
                }
                body.clear();
            } else {
                active_info = Some(info.trim().to_ascii_lowercase());
            }
            continue;
        }
        if active_info.is_some() {
            body.push(line);
        }
    }

    (task_fences, json_fences)
}

fn is_task_result_fence(info: &str) -> bool {
    info.contains("task-result")
        || info.contains("task_result")
        || info
            .split_whitespace()
            .collect::<Vec<_>>()
            .windows(2)
            .any(|window| window[0] == "task" && window[1] == "result")
}

fn is_json_result_fence(info: &str, contract: Option<&TaskAgentResultContract>) -> bool {
    matches!(
        contract.map(|contract| contract.format),
        Some(TaskAgentResultFormat::Json)
    ) && info.split_whitespace().next() == Some("json")
}

fn validate_structured_candidate(
    value: &TaskValue,
    contract: Option<&TaskAgentResultContract>,
) -> Vec<String> {
    let Some(contract) = contract else {
        return Vec::new();
    };
    let mut errors = Vec::new();
    if matches!(contract.format, TaskAgentResultFormat::Json)
        && !matches!(value, TaskValue::Object(_) | TaskValue::List(_))
    {
        errors.push("result contract expects JSON object or array".to_owned());
    }
    if let Some(schema) = contract.schema.as_ref() {
        errors.extend(validate_task_value_schema(
            structured_contract_validation_target(value),
            &schema.schema,
            "$",
        ));
    }
    errors
}

fn structured_contract_validation_target(value: &TaskValue) -> &TaskValue {
    if let TaskValue::Object(object) = value
        && let Some(data) = object.get("data")
    {
        return data;
    }
    value
}

fn validate_task_value_schema(value: &TaskValue, schema: &TaskValue, path: &str) -> Vec<String> {
    let Some(schema_object) = task_value_object(schema) else {
        return Vec::new();
    };
    let mut errors = Vec::new();

    if let Some(type_schema) = schema_object.get("type")
        && !schema_type_matches(value, type_schema)
    {
        errors.push(format!(
            "{path} does not match schema type {}",
            schema_type_label(type_schema)
        ));
        return errors;
    }

    if let Some(enum_schema) = schema_object.get("enum")
        && let TaskValue::List(values) = enum_schema
        && !values.iter().any(|allowed| allowed == value)
    {
        errors.push(format!("{path} is not one of the allowed enum values"));
    }

    if let TaskValue::Object(object) = value {
        if let Some(TaskValue::List(required)) = schema_object.get("required") {
            for name in required.iter().filter_map(task_value_str) {
                if !object.contains_key(name) {
                    errors.push(format!("{path}.{name} is required"));
                }
            }
        }
        if let Some(TaskValue::Object(properties)) = schema_object.get("properties") {
            for (name, property_schema) in properties {
                if let Some(child) = object.get(name) {
                    errors.extend(validate_task_value_schema(
                        child,
                        property_schema,
                        format!("{path}.{name}").as_str(),
                    ));
                }
            }
        }
    }

    if let TaskValue::List(items) = value
        && let Some(item_schema) = schema_object.get("items")
    {
        for (index, item) in items.iter().enumerate() {
            errors.extend(validate_task_value_schema(
                item,
                item_schema,
                format!("{path}[{index}]").as_str(),
            ));
        }
    }

    errors
}

fn schema_type_matches(value: &TaskValue, type_schema: &TaskValue) -> bool {
    match type_schema {
        TaskValue::String(type_name) => task_value_matches_type(value, type_name.as_str()),
        TaskValue::List(type_names) => type_names
            .iter()
            .filter_map(task_value_str)
            .any(|type_name| task_value_matches_type(value, type_name)),
        _ => true,
    }
}

fn task_value_matches_type(value: &TaskValue, type_name: &str) -> bool {
    match type_name {
        "null" => matches!(value, TaskValue::Null),
        "boolean" => matches!(value, TaskValue::Bool(_)),
        "integer" => matches!(value, TaskValue::Integer(_)),
        "number" => matches!(value, TaskValue::Integer(_) | TaskValue::Number(_)),
        "string" => matches!(value, TaskValue::String(_)),
        "array" => matches!(value, TaskValue::List(_)),
        "object" => matches!(value, TaskValue::Object(_)),
        _ => true,
    }
}

fn schema_type_label(type_schema: &TaskValue) -> String {
    match type_schema {
        TaskValue::String(value) => value.clone(),
        TaskValue::List(values) => values
            .iter()
            .filter_map(task_value_str)
            .collect::<Vec<_>>()
            .join("|"),
        _ => "unknown".to_owned(),
    }
}

fn strip_structured_result_blocks(raw_text: &str) -> String {
    let mut stripped = strip_tagged_blocks(raw_text, "<task_result>", "</task_result>");
    stripped = strip_tagged_blocks(stripped.as_str(), "<task-result>", "</task-result>");
    strip_fenced_result_blocks(stripped.as_str())
}

fn strip_tagged_blocks(raw_text: &str, open_tag: &str, close_tag: &str) -> String {
    let lower = raw_text.to_ascii_lowercase();
    let mut ranges = Vec::new();
    let mut cursor = 0;
    while let Some(open_offset) = lower[cursor..].find(open_tag) {
        let start = cursor + open_offset;
        let content_start = start + open_tag.len();
        let Some(close_offset) = lower[content_start..].find(close_tag) else {
            break;
        };
        let end = content_start + close_offset + close_tag.len();
        ranges.push((start, end));
        cursor = end;
    }
    remove_ranges(raw_text, ranges)
}

fn strip_fenced_result_blocks(raw_text: &str) -> String {
    let mut output = Vec::new();
    let mut active_info: Option<String> = None;
    let mut active_lines = Vec::new();
    for line in raw_text.lines() {
        let trimmed = line.trim_start();
        if let Some(info) = trimmed.strip_prefix("```") {
            if let Some(open_info) = active_info.take() {
                if !is_task_result_fence(open_info.as_str())
                    && !open_info
                        .split_whitespace()
                        .next()
                        .is_some_and(|value| value == "json")
                {
                    output.append(&mut active_lines);
                    output.push(line.to_owned());
                }
                active_lines.clear();
            } else {
                let open_info = info.trim().to_ascii_lowercase();
                if !is_task_result_fence(open_info.as_str())
                    && !open_info
                        .split_whitespace()
                        .next()
                        .is_some_and(|value| value == "json")
                {
                    active_lines.push(line.to_owned());
                }
                active_info = Some(open_info);
            }
            continue;
        }
        if active_info.is_some() {
            active_lines.push(line.to_owned());
        } else {
            output.push(line.to_owned());
        }
    }
    if !active_lines.is_empty() {
        output.append(&mut active_lines);
    }
    output.join("\n").trim().to_owned()
}

fn remove_ranges(raw_text: &str, ranges: Vec<(usize, usize)>) -> String {
    if ranges.is_empty() {
        return raw_text.to_owned();
    }
    let mut output = String::new();
    let mut cursor = 0;
    for (start, end) in ranges {
        output.push_str(&raw_text[cursor..start]);
        cursor = end;
    }
    output.push_str(&raw_text[cursor..]);
    output.trim().to_owned()
}

fn task_value_from_json(value: serde_json::Value) -> TaskValue {
    match value {
        serde_json::Value::Null => TaskValue::Null,
        serde_json::Value::Bool(value) => TaskValue::Bool(value),
        serde_json::Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                TaskValue::Integer(integer)
            } else {
                TaskValue::Number(value.as_f64().unwrap_or_default())
            }
        }
        serde_json::Value::String(value) => TaskValue::String(value),
        serde_json::Value::Array(values) => {
            TaskValue::List(values.into_iter().map(task_value_from_json).collect())
        }
        serde_json::Value::Object(values) => TaskValue::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, task_value_from_json(value)))
                .collect(),
        ),
    }
}

fn task_value_object(value: &TaskValue) -> Option<&BTreeMap<String, TaskValue>> {
    match value {
        TaskValue::Object(value) => Some(value),
        _ => None,
    }
}

fn task_value_str(value: &TaskValue) -> Option<&str> {
    match value {
        TaskValue::String(value) => Some(value.as_str()),
        _ => None,
    }
}

fn task_value_into_string(value: TaskValue) -> Option<String> {
    match value {
        TaskValue::String(value) => Some(value),
        _ => None,
    }
}

async fn load_workspace_skill_policies(
    processor: &Arc<MessageProcessor>,
    workspace_id: &str,
) -> std::collections::HashMap<pioneer_skills::SkillPolicyKey, pioneer_agent::WorkspaceSkillPolicy>
{
    match processor
        .crud_store
        .list_workspace_skill_policies(workspace_id)
        .await
    {
        Ok(records) => records
            .into_iter()
            .map(|record| {
                (
                    pioneer_skills::SkillPolicyKey::new(record.skill_slug, record.source_kind),
                    pioneer_agent::WorkspaceSkillPolicy {
                        enabled: record.enabled,
                        allow_implicit_invocation: record.allow_implicit_invocation,
                    },
                )
            })
            .collect(),
        Err(error) => {
            warn!(
                workspace_id,
                error = %format!("{error:#}"),
                "failed to load workspace skill policies for task child turn; continuing"
            );
            std::collections::HashMap::new()
        }
    }
}

fn task_error(
    code: impl Into<String>,
    message: impl Into<String>,
    class: TaskErrorClass,
    failed_run_id: Option<String>,
) -> TaskError {
    TaskError {
        code: code.into(),
        message: message.into(),
        class,
        details: None,
        failed_run_id,
    }
}

fn first_meaningful_line(value: &str) -> Option<String> {
    value
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use migration::{Migrator, MigratorTrait};
    use pioneer_agent::{
        SkillsDependenciesLoopConfig, SkillsLoopConfig, SkillsRuntimeLoopConfig,
        SkillsSecurityLoopConfig, SkillsValidationLoopConfig,
    };
    use pioneer_artifacts::{
        ArtifactBindingTarget, ArtifactListFilter, IngestArtifactBytesRequest,
    };
    use pioneer_config::GatewayWebToolsConfig;
    use pioneer_keystore::MemorySecretStore;
    use pioneer_memory::hooks::{
        MemoryActiveRecallConfig, MemoryActiveRecallMode, MemoryLoopConfig,
    };
    use pioneer_protocol::{
        ArtifactBindingDirection, ArtifactBindingKind, ArtifactCreatedByKind, ArtifactKind,
        ArtifactRole, ArtifactSummary, TaskAgentInputAttachmentKind, TaskAgentInputReferenceKind,
        TaskArtifact,
    };
    use pioneer_provider::providers::EchoProvider;
    use pioneer_tools::{
        ComputerUseToolsConfig, ToolLoopBudgetConfig, ToolRetryBudgetConfig, WebToolsConfig,
    };
    use sea_orm::Database;

    fn test_task_run_turn() -> TaskRunTurn {
        TaskRunTurn {
            id: "task_run_turn".to_owned(),
            task_id: "task".to_owned(),
            run_id: "run".to_owned(),
            execution_id: Some("execution".to_owned()),
            thread_id: "child_thread".to_owned(),
            turn_id: "child_turn".to_owned(),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: 1,
            started_at: Some(1),
            completed_at: Some(2),
        }
    }

    fn permission_test_agent_spec(
        permission_cap: Option<pioneer_protocol::TurnPermissionProfileCap>,
        tool_policy: Option<TaskAgentToolPolicy>,
    ) -> TaskAgentSpec {
        TaskAgentSpec {
            id: "agent_spec_permission".to_owned(),
            task_id: "task_permission".to_owned(),
            run_id: None,
            agent_role: None,
            agent_nickname: None,
            model: Some("test-model".to_owned()),
            model_provider: Some("openai".to_owned()),
            prompt: pioneer_protocol::TaskAgentPrompt {
                goal: "Do the task".to_owned(),
                instructions: Vec::new(),
                input: None,
                output_instructions: None,
            },
            context_policy: None,
            tool_policy,
            permission_cap,
            result_contract: None,
            review_policy: None,
            depth: 0,
            max_depth: 3,
            created_at: 1,
            updated_at: 1,
        }
    }

    #[test]
    fn task_child_permission_profile_rejects_missing_cap() {
        let agent_spec = permission_test_agent_spec(None, None);
        let error = effective_task_child_permission_profile(&agent_spec, None)
            .expect_err("missing permission cap should fail");

        assert!(format!("{error:#}").contains("missing permission_cap"));
    }

    #[test]
    fn task_child_permission_profile_inherits_parent_cap_modes() {
        for mode in [
            pioneer_protocol::TurnPermissionMode::FullAccess,
            pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
            pioneer_protocol::TurnPermissionMode::Supervised,
        ] {
            let agent_spec = permission_test_agent_spec(
                Some(pioneer_protocol::task_permission_cap_from_snapshot(
                    &pioneer_protocol::inherited_turn_permission_profile_snapshot(mode),
                )),
                None,
            );
            let profile = effective_task_child_permission_profile(&agent_spec, None);
            let profile = profile.expect("permission cap should produce a profile");

            assert_eq!(profile.mode, mode);
            assert_eq!(
                profile.source,
                TurnPermissionProfileSource::TaskPermissionCap
            );
        }
    }

    #[test]
    fn task_child_permission_profile_uses_most_restrictive_cap_and_launch_mode() {
        let agent_spec = permission_test_agent_spec(
            Some(pioneer_protocol::task_permission_cap_for_mode(
                pioneer_protocol::TurnPermissionMode::AutoAcceptEdits,
            )),
            None,
        );
        let launch_profile = pioneer_protocol::inherited_turn_permission_profile_snapshot(
            pioneer_protocol::TurnPermissionMode::Supervised,
        );
        let profile = effective_task_child_permission_profile(&agent_spec, Some(&launch_profile))
            .expect("permission cap should produce a profile");

        assert_eq!(
            profile.mode,
            pioneer_protocol::TurnPermissionMode::Supervised
        );
        assert_eq!(profile.effective_policy.file_write, PermissionBehavior::Ask);
    }

    #[test]
    fn delayed_task_permission_cap_cannot_be_broadened_by_full_access_launch() {
        let agent_spec = permission_test_agent_spec(
            Some(pioneer_protocol::task_permission_cap_for_mode(
                pioneer_protocol::TurnPermissionMode::Supervised,
            )),
            None,
        );
        let launch_profile = pioneer_protocol::system_turn_permission_profile_snapshot(
            pioneer_protocol::TurnPermissionMode::FullAccess,
        );
        let profile = effective_task_child_permission_profile(&agent_spec, Some(&launch_profile))
            .expect("permission cap should produce a profile");

        assert_eq!(
            profile.mode,
            pioneer_protocol::TurnPermissionMode::Supervised
        );
    }

    #[test]
    fn task_tool_policy_narrows_effective_child_permission_profile() {
        let agent_spec = permission_test_agent_spec(
            Some(pioneer_protocol::task_permission_cap_for_mode(
                pioneer_protocol::TurnPermissionMode::FullAccess,
            )),
            Some(TaskAgentToolPolicy {
                allowed_tools: vec!["read_file".to_owned()],
                denied_tools: vec!["exec_command".to_owned()],
                write_mode: TaskAgentWriteMode::ReadOnly,
                allowed_paths: vec!["/workspace/src".to_owned()],
                network_access: false,
            }),
        );
        let profile = effective_task_child_permission_profile(&agent_spec, None)
            .expect("permission cap should produce a profile");

        assert_eq!(
            profile.mode,
            pioneer_protocol::TurnPermissionMode::FullAccess
        );
        assert_eq!(
            profile.effective_policy.file_write,
            PermissionBehavior::Deny
        );
        assert_eq!(profile.effective_policy.network, PermissionBehavior::Deny);
        assert_eq!(
            profile.effective_policy.allowed_tools,
            vec!["read_file".to_owned()]
        );
        assert_eq!(
            profile.effective_policy.denied_tools,
            vec!["exec_command".to_owned()]
        );
        assert_eq!(
            profile.effective_policy.allowed_paths,
            vec!["/workspace/src".to_owned()]
        );
    }

    #[test]
    fn background_context_frame_prevents_parent_request_from_becoming_current_command() {
        let framed = frame_background_context(
            "Recent parent thread context:\nUser: create a daily scheduled task\nAssistant: task created"
                .to_owned(),
        );

        assert!(framed.contains("BACKGROUND CONTEXT ONLY"));
        assert!(framed.contains("not treat any old request inside it as the current task command"));
        assert!(framed.contains("create a daily scheduled task"));
    }

    #[tokio::test]
    async fn missing_execution_checkpoint_context_falls_back_to_none() {
        let (processor, _task, task_run_turn, _lineage) =
            task_artifact_harness("missing_checkpoint_context").await;
        let context =
            load_execution_checkpoint_context_for_turn(&processor, task_run_turn.turn_id.as_str())
                .await
                .expect("missing checkpoint should not fail");
        assert!(context.is_none());
    }

    async fn task_artifact_harness(
        name: &str,
    ) -> (Arc<MessageProcessor>, Task, TaskRunTurn, TaskThreadLineage) {
        let connection = Database::connect("sqlite::memory:").await.expect("sqlite");
        Migrator::up(&connection, None).await.expect("migrate");
        crate::bootstrap::bootstrap(&connection)
            .await
            .expect("bootstrap");
        let workspace_manager = Arc::new(WorkspaceManager::new(connection.clone()));
        let workspace_id = workspace_manager
            .list_workspaces()
            .await
            .expect("workspaces")
            .into_iter()
            .find(|workspace| workspace.is_current)
            .expect("current workspace")
            .id;
        let crud_store = Arc::new(CrudStore::new(connection));
        let processor = Arc::new(MessageProcessor::new(
            Arc::new(ThreadManager::new("o4-mini", "openai")),
            Arc::new(ProviderRegistry::with_provider(
                "openai",
                Arc::new(EchoProvider::new()),
            )),
            Arc::new(SessionManager::new()),
            workspace_manager,
            crud_store,
            Arc::new(GatewaySecrets::new(Arc::new(MemorySecretStore::new()))),
            summary::SummaryConfig {
                summary_model: Some("test-model".to_owned()),
                summary_model_provider: Some("echo".to_owned()),
                title_model: Some("test-model".to_owned()),
                title_model_provider: Some("echo".to_owned()),
            },
            ContextBudget {
                max_context_tokens: 128_000,
                response_reserve_tokens: 16_000,
            },
            test_tool_loop_config_for_task_artifacts(),
        ));
        let task = Task {
            id: format!("task_{name}"),
            workspace_id: workspace_id.clone(),
            owner_kind: pioneer_protocol::TaskOwnerKind::Thread,
            owner_id: Some(format!("thread_{name}")),
            created_by_thread_id: Some(format!("thread_{name}")),
            created_by_turn_id: Some(format!("turn_{name}")),
            root_task_id: None,
            parent_task_id: None,
            executor_kind: TaskExecutorKind::Agent,
            status: pioneer_protocol::TaskStatus::Running,
            title: "Task".to_owned(),
            goal: "Goal".to_owned(),
            priority: 0,
            lifecycle_policy: None,
            delivery_policy: None,
            retry_policy: None,
            timeout_policy: None,
            concurrency_policy: None,
            metadata: None,
            result: None,
            error: None,
            revision: 1,
            created_at: 1,
            updated_at: 1,
            completed_at: None,
        };
        let task_run_turn = TaskRunTurn {
            id: format!("task_run_turn_{name}"),
            task_id: task.id.clone(),
            run_id: format!("run_{name}"),
            execution_id: Some(format!("execution_{name}")),
            thread_id: format!("child_{name}"),
            turn_id: format!("child_turn_{name}"),
            kind: TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: 1,
            started_at: Some(1),
            completed_at: Some(2),
        };
        let lineage = TaskThreadLineage {
            child_thread_id: format!("child_{name}"),
            parent_thread_id: format!("thread_{name}"),
            root_thread_id: format!("thread_{name}"),
            depth: 1,
            origin_kind: Some("task_run".to_owned()),
            created_by_thread_id: Some(format!("thread_{name}")),
            created_by_turn_id: Some(format!("turn_{name}")),
            created_at: 1,
        };
        (processor, task, task_run_turn, lineage)
    }

    fn test_tool_loop_config_for_task_artifacts() -> ToolLoopConfig {
        let web = GatewayWebToolsConfig::default();
        ToolLoopConfig {
            provider: pioneer_provider::ProviderTimeoutPolicy::default(),
            preflight: pioneer_agent::PreflightLoopConfig::default(),
            web: WebToolsConfig {
                default_timeout_ms: web.default_timeout_ms,
                hard_max_timeout_ms: web.hard_max_timeout_ms,
                default_fetch_max_bytes: web.default_fetch_max_bytes,
                hard_fetch_max_bytes: web.hard_fetch_max_bytes,
                default_download_max_bytes: web.default_download_max_bytes,
                hard_download_max_bytes: web.hard_download_max_bytes,
                default_max_results: web.default_max_results,
                hard_max_results: web.hard_max_results,
                default_snippet_chars: web.default_snippet_chars,
                hard_max_snippet_chars: web.hard_max_snippet_chars,
                default_link_count: web.default_link_count,
                hard_link_count: web.hard_link_count,
                default_render_max_chars: web.default_render_max_chars,
                ddg_html_search_url: web.ddg_html_search_url,
                ddg_instant_api_url: web.ddg_instant_api_url,
                default_user_agent: web.default_user_agent,
            },
            computer_use: ComputerUseToolsConfig {
                runtime_home_dir: std::env::temp_dir().join("pioneer-task-artifact-tests"),
                artifacts_subdir: "tools/computer_use".to_owned(),
                ..ComputerUseToolsConfig::default()
            },
            skills: SkillsLoopConfig {
                enabled: true,
                max_skills_per_source: 256,
                max_skill_file_bytes: 1024 * 1024,
                prompt_max_chars: 24_000,
                allow_implicit_invocation: false,
                system_roots: Vec::new(),
                user_roots: Vec::new(),
                registry_roots: Vec::new(),
                validation: SkillsValidationLoopConfig {
                    strict_agentskills: true,
                    accept_openclaw_profile: true,
                },
                security: SkillsSecurityLoopConfig {
                    allow_untrusted_install: false,
                    min_trust_for_shell_tools: pioneer_skills::SkillTrustLevel::Verified,
                    min_trust_for_http_tools: pioneer_skills::SkillTrustLevel::Community,
                    min_trust_for_function_proxy_tools: pioneer_skills::SkillTrustLevel::Community,
                    max_install_archive_bytes: 10 * 1024 * 1024,
                    max_install_archive_compressed_bytes: 10 * 1024 * 1024,
                    max_install_archive_uncompressed_bytes: 50 * 1024 * 1024,
                    max_install_archive_entries: 2048,
                    max_install_file_bytes: 1024 * 1024,
                    upload_ttl_secs: 3600,
                    upload_recommended_chunk_size_bytes: 256 * 1024,
                    upload_max_chunk_size_bytes: 1024 * 1024,
                },
                dependencies: SkillsDependenciesLoopConfig {
                    preflight_on_resolve: true,
                    runtime_recheck_on_tool_call: true,
                },
                runtime: SkillsRuntimeLoopConfig {
                    enable_dynamic_tools: true,
                    enable_read_skill: true,
                    max_dynamic_tools_per_skill: 64,
                    read_skill_max_chars: 72_000,
                    compact_mode_threshold: 6,
                    allow_shell_tools: true,
                    allow_http_tools: true,
                    allow_function_proxy_tools: true,
                },
            },
            memory: MemoryLoopConfig {
                active_recall: MemoryActiveRecallConfig {
                    mode: MemoryActiveRecallMode::DeterministicOnly,
                    ..MemoryActiveRecallConfig::default()
                },
                ..MemoryLoopConfig::default()
            },
            budget: ToolLoopBudgetConfig::default(),
            execution_windows: pioneer_tools::ExecutionWindowsConfig::default(),
            retry: ToolRetryBudgetConfig::default(),
        }
        .normalized()
    }

    async fn ingest_task_test_artifact(
        processor: &MessageProcessor,
        workspace_id: &str,
        thread_id: Option<String>,
        display_name: &str,
    ) -> ArtifactSummary {
        processor
            .artifact_service
            .ingest_bytes(IngestArtifactBytesRequest {
                workspace_id: workspace_id.to_owned(),
                primary_thread_id: thread_id.clone(),
                bytes: b"task artifact".to_vec(),
                display_name: display_name.to_owned(),
                kind: ArtifactKind::Text,
                mime_type: Some("text/plain".to_owned()),
                created_by_kind: ArtifactCreatedByKind::User,
                created_by_actor_id: None,
                binding: thread_id.map(|thread_id| ArtifactBindingTarget {
                    thread_id: Some(thread_id),
                    turn_id: None,
                    message_id: None,
                    turn_item_id: None,
                    tool_call_id: None,
                    task_id: None,
                    task_run_id: None,
                    binding_kind: ArtifactBindingKind::ManualAttach,
                    direction: ArtifactBindingDirection::Context,
                    role: Some(ArtifactRole::User),
                    item_index: None,
                }),
                metadata: Default::default(),
            })
            .await
            .expect("ingest artifact")
    }

    fn json_answer_contract() -> TaskAgentResultContract {
        TaskAgentResultContract {
            format: TaskAgentResultFormat::Json,
            required: true,
            schema: Some(pioneer_protocol::TaskSchema {
                name: Some("answer".to_owned()),
                description: None,
                schema: TaskValue::Object(BTreeMap::from([
                    ("type".to_owned(), TaskValue::String("object".to_owned())),
                    (
                        "required".to_owned(),
                        TaskValue::List(vec![TaskValue::String("answer".to_owned())]),
                    ),
                    (
                        "properties".to_owned(),
                        TaskValue::Object(BTreeMap::from([(
                            "answer".to_owned(),
                            TaskValue::Object(BTreeMap::from([(
                                "type".to_owned(),
                                TaskValue::String("string".to_owned()),
                            )])),
                        )])),
                    ),
                ])),
            }),
        }
    }

    #[test]
    fn result_extractor_prefers_valid_structured_task_result_block() {
        let raw_text = r#"Human readable fallback.
```task-result
{"summary":"Structured summary","data":{"answer":"42"}}
```"#;

        let result = TaskAgentResultExtractor::normalize_final_message(
            raw_text.to_owned(),
            "item".to_owned(),
            &test_task_run_turn(),
            Some(&json_answer_contract()),
        )
        .expect("structured result should be valid");

        assert_eq!(result.summary.as_deref(), Some("Structured summary"));
        let TaskValue::Object(data) = result.data.expect("data should be present") else {
            panic!("structured data should remain an object");
        };
        assert_eq!(
            data.get("answer"),
            Some(&TaskValue::String("42".to_owned()))
        );
        assert_eq!(result.completed_by_run_id.as_deref(), Some("run"));
    }

    #[test]
    fn task_artifact_input_attachments_are_materialized_as_user_inputs() {
        let agent_spec = TaskAgentSpec {
            id: "spec".to_owned(),
            task_id: "task".to_owned(),
            run_id: None,
            agent_role: None,
            agent_nickname: None,
            model: Some("model".to_owned()),
            model_provider: Some("provider".to_owned()),
            prompt: pioneer_protocol::TaskAgentPrompt {
                goal: "Use artifacts".to_owned(),
                instructions: Vec::new(),
                input: Some(TaskAgentInput {
                    text: None,
                    variables: Vec::new(),
                    attachments: vec![pioneer_protocol::TaskAgentInputAttachment {
                        kind: TaskAgentInputAttachmentKind::Artifact,
                        name: None,
                        path: None,
                        url: None,
                        artifact_id: Some("art_input".to_owned()),
                        mime_type: None,
                    }],
                    references: vec![pioneer_protocol::TaskAgentInputReference {
                        kind: TaskAgentInputReferenceKind::Artifact,
                        id: "art_ref".to_owned(),
                        label: None,
                    }],
                }),
                output_instructions: None,
            },
            context_policy: None,
            tool_policy: None,
            permission_cap: Some(pioneer_protocol::task_permission_cap_from_snapshot(
                &pioneer_protocol::default_turn_permission_profile_snapshot(),
            )),
            result_contract: None,
            review_policy: None,
            depth: 0,
            max_depth: 1,
            created_at: 1,
            updated_at: 1,
        };

        let inputs = materialize_child_task_input("prompt".to_owned(), &agent_spec);

        assert!(matches!(inputs[0], UserInput::Text { .. }));
        assert!(inputs.iter().any(|input| matches!(
            input,
            UserInput::Artifact { artifact_id, .. } if artifact_id == "art_input"
        )));
        assert!(inputs.iter().any(|input| matches!(
            input,
            UserInput::Artifact { artifact_id, .. } if artifact_id == "art_ref"
        )));
    }

    #[test]
    fn task_artifact_parser_preserves_version_id() {
        let values = vec![TaskValue::Object(BTreeMap::from([
            (
                "artifactId".to_owned(),
                TaskValue::String("artifact".to_owned()),
            ),
            (
                "versionId".to_owned(),
                TaskValue::String("version".to_owned()),
            ),
        ]))];

        let artifacts = task_artifacts::parse_task_artifacts(&values);

        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].artifact_id.as_deref(), Some("artifact"));
        assert_eq!(artifacts[0].version_id.as_deref(), Some("version"));
    }

    #[test]
    fn reviewer_advisory_parser_reads_json_decision_feedback_and_confidence() {
        let advisory = parse_reviewer_advisory_text(
            r#"{"decision":"request_changes","feedback":"tighten the summary","confidence":0.8}"#,
        );

        assert_eq!(advisory.decision, TaskResultReviewDecision::RequestChanges);
        assert_eq!(
            advisory.feedback_text.as_deref(),
            Some("tighten the summary")
        );
        assert_eq!(advisory.confidence, Some(0.8));
        assert!(matches!(advisory.feedback, Some(TaskValue::Object(_))));
    }

    #[test]
    fn reviewer_advisory_parser_falls_back_to_text_decision() {
        let advisory = parse_reviewer_advisory_text("I would accept this result.");

        assert_eq!(advisory.decision, TaskResultReviewDecision::Accept);
        assert_eq!(
            advisory.feedback_text.as_deref(),
            Some("I would accept this result.")
        );
    }

    #[tokio::test]
    async fn task_artifact_existing_id_gets_task_result_binding() {
        let (processor, task, task_run_turn, lineage) = task_artifact_harness("existing").await;
        let source =
            ingest_task_test_artifact(&processor, task.workspace_id.as_str(), None, "source.txt")
                .await;
        let result = TaskResult {
            summary: Some("done".to_owned()),
            data: None,
            artifacts: vec![TaskArtifact {
                artifact_id: Some(source.artifact.artifact_id.clone()),
                version_id: source.artifact.version_id.clone(),
                path: None,
                url: None,
                mime_type: None,
                metadata: None,
            }],
            completed_by_run_id: Some(task_run_turn.run_id.clone()),
        };

        let normalized = task_artifacts::normalize_task_result_artifacts(
            &processor,
            &task,
            &task_run_turn,
            &lineage,
            result,
        )
        .await
        .expect("normalize")
        .expect("valid result");

        assert_eq!(
            normalized.artifacts[0].artifact_id.as_deref(),
            Some(source.artifact.artifact_id.as_str())
        );
        let summary = processor
            .artifact_service
            .get_artifact(
                task.workspace_id.as_str(),
                source.artifact.artifact_id.as_str(),
                None,
            )
            .await
            .expect("artifact");
        assert!(summary.bindings.iter().any(|binding| {
            binding.binding_kind == ArtifactBindingKind::TaskResult
                && binding.task_id.as_deref() == Some(task.id.as_str())
                && binding.task_run_id.as_deref() == Some(task_run_turn.run_id.as_str())
                && binding.thread_id.as_deref() == task.created_by_thread_id.as_deref()
        }));
    }

    #[tokio::test]
    async fn review_candidate_artifact_existing_id_gets_task_result_candidate_binding() {
        let (processor, task, task_run_turn, lineage) =
            task_artifact_harness("candidate_existing").await;
        let source = ingest_task_test_artifact(
            &processor,
            task.workspace_id.as_str(),
            None,
            "candidate-source.txt",
        )
        .await;
        let candidate_id = "candidate_artifact_binding";
        let result = TaskResult {
            summary: Some("candidate".to_owned()),
            data: None,
            artifacts: vec![TaskArtifact {
                artifact_id: Some(source.artifact.artifact_id.clone()),
                version_id: source.artifact.version_id.clone(),
                path: None,
                url: None,
                mime_type: None,
                metadata: None,
            }],
            completed_by_run_id: Some(task_run_turn.run_id.clone()),
        };

        let normalized = task_artifacts::normalize_task_result_candidate_artifacts(
            &processor,
            &task,
            &task_run_turn,
            &lineage,
            candidate_id,
            result,
        )
        .await
        .expect("normalize")
        .expect("valid result");

        assert_eq!(
            normalized.artifacts[0].artifact_id.as_deref(),
            Some(source.artifact.artifact_id.as_str())
        );
        let summary = processor
            .artifact_service
            .get_artifact(
                task.workspace_id.as_str(),
                source.artifact.artifact_id.as_str(),
                None,
            )
            .await
            .expect("artifact");
        assert!(summary.bindings.iter().any(|binding| {
            binding.binding_kind == ArtifactBindingKind::TaskResultCandidate
                && binding.task_id.as_deref() == Some(task.id.as_str())
                && binding.task_run_id.as_deref() == Some(task_run_turn.run_id.as_str())
                && binding.thread_id.as_deref() == task.created_by_thread_id.as_deref()
                && binding.item_index == Some(0)
        }));
        assert!(!summary.bindings.iter().any(|binding| {
            binding.binding_kind == ArtifactBindingKind::TaskResult
                && binding.task_id.as_deref() == Some(task.id.as_str())
                && binding.task_run_id.as_deref() == Some(task_run_turn.run_id.as_str())
        }));
        let page = processor
            .artifact_service
            .list_artifacts(
                task.workspace_id.as_str(),
                ArtifactListFilter {
                    task_id: Some(task.id.clone()),
                    task_run_id: Some(task_run_turn.run_id.clone()),
                    ..ArtifactListFilter::default()
                },
            )
            .await
            .expect("list task artifacts");
        assert_eq!(page.items.len(), 1);
        assert!(
            page.items[0]
                .bindings
                .iter()
                .any(|binding| binding.binding_kind == ArtifactBindingKind::TaskResultCandidate)
        );
    }

    #[tokio::test]
    async fn task_artifact_path_is_ingested_and_listable_by_task() {
        let (processor, task, task_run_turn, lineage) = task_artifact_harness("path").await;
        let output_dir = std::env::current_dir()
            .expect("cwd")
            .join("target")
            .join("task-artifact-tests")
            .join(task_run_turn.run_id.as_str());
        tokio::fs::create_dir_all(output_dir.as_path())
            .await
            .expect("mkdir");
        let output_path = output_dir.join("result.txt");
        tokio::fs::write(output_path.as_path(), b"path artifact")
            .await
            .expect("write");
        let result = TaskResult {
            summary: Some("done".to_owned()),
            data: None,
            artifacts: vec![TaskArtifact {
                artifact_id: None,
                version_id: None,
                path: Some(output_path.display().to_string()),
                url: None,
                mime_type: Some("text/plain".to_owned()),
                metadata: None,
            }],
            completed_by_run_id: Some(task_run_turn.run_id.clone()),
        };

        let normalized = task_artifacts::normalize_task_result_artifacts(
            &processor,
            &task,
            &task_run_turn,
            &lineage,
            result,
        )
        .await
        .expect("normalize")
        .expect("valid result");

        assert!(normalized.artifacts[0].artifact_id.is_some());
        let page = processor
            .artifact_service
            .list_artifacts(
                task.workspace_id.as_str(),
                ArtifactListFilter {
                    task_id: Some(task.id.clone()),
                    task_run_id: Some(task_run_turn.run_id.clone()),
                    ..ArtifactListFilter::default()
                },
            )
            .await
            .expect("list task artifacts");
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].artifact.display_name, "result.txt");
        let _ = tokio::fs::remove_dir_all(output_dir.as_path()).await;
    }

    #[tokio::test]
    async fn task_artifact_rejects_cross_workspace_id() {
        let (processor, task, task_run_turn, lineage) = task_artifact_harness("foreign").await;
        let other_workspace = processor
            .workspace_manager
            .create_workspace("task_artifact_other", Some("Task Artifact Other"))
            .await
            .expect("other workspace");
        let source =
            ingest_task_test_artifact(&processor, other_workspace.id.as_str(), None, "foreign.txt")
                .await;
        let result = TaskResult {
            summary: None,
            data: None,
            artifacts: vec![TaskArtifact {
                artifact_id: Some(source.artifact.artifact_id),
                version_id: source.artifact.version_id,
                path: None,
                url: None,
                mime_type: None,
                metadata: None,
            }],
            completed_by_run_id: Some(task_run_turn.run_id.clone()),
        };

        let error = task_artifacts::normalize_task_result_artifacts(
            &processor,
            &task,
            &task_run_turn,
            &lineage,
            result,
        )
        .await
        .expect("normalization should return task error")
        .expect_err("foreign artifact should fail result");

        assert_eq!(error.code, "task_artifact_invalid");
    }

    #[tokio::test]
    async fn include_artifacts_context_renders_refs_without_paths() {
        let (processor, task, _task_run_turn, lineage) = task_artifact_harness("context").await;
        let source = ingest_task_test_artifact(
            &processor,
            task.workspace_id.as_str(),
            task.created_by_thread_id.clone(),
            "context.txt",
        )
        .await;
        let rendered = task_artifacts::render_parent_artifact_refs(
            &processor,
            task.workspace_id.as_str(),
            &TaskParentRuntimeContext {
                parent_thread_id: lineage.parent_thread_id,
                parent_turn_id: lineage.created_by_turn_id,
                root_thread_id: lineage.root_thread_id,
            },
        )
        .await
        .expect("render")
        .expect("artifact context");

        assert!(rendered.contains(source.artifact.artifact_id.as_str()));
        assert!(rendered.contains("context.txt"));
        assert!(!rendered.contains("source_path"));
    }

    #[test]
    fn result_extractor_falls_back_when_structured_block_violates_schema() {
        let raw_text = r#"Plain fallback result.
```task-result
{"summary":"Bad structured summary","data":{"answer":42}}
```"#;

        let result = TaskAgentResultExtractor::normalize_final_message(
            raw_text.to_owned(),
            "item".to_owned(),
            &test_task_run_turn(),
            Some(&json_answer_contract()),
        )
        .expect("invalid structured result should fallback to text");

        assert_eq!(result.summary.as_deref(), Some("Plain fallback result."));
        let TaskValue::Object(data) = result.data.expect("data should be present") else {
            panic!("fallback data should be an object");
        };
        assert_eq!(data.get("fallbackUsed"), Some(&TaskValue::Bool(true)));
        let diagnostics = data.get("diagnostics").expect("diagnostics should exist");
        let TaskValue::List(diagnostics) = diagnostics else {
            panic!("diagnostics should be a list");
        };
        assert!(!diagnostics.is_empty());
    }
}
