use super::*;
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use pioneer_protocol::{
    SandboxMode, Task, TaskAgentContext, TaskAgentContextMode, TaskAgentInput, TaskAgentPrompt,
    TaskAgentResultContract, TaskAgentResultFormat, TaskAgentSpec, TaskArtifact, TaskError,
    TaskErrorClass, TaskExecutorKind, TaskGetResponse, TaskResult, TaskRun, TaskRunStatus,
    TaskValue, ThreadLineage, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility,
    TurnStartParams, TurnStatus, UserInput, generate_id,
};
use pioneer_tasks::{
    TaskExecutionContext, TaskExecutionHandle, TaskExecutor, TaskExecutorRecoveryOutcome,
    TaskExecutorStartOutcome, WriteLockDecision,
};
use std::collections::BTreeMap;
use std::sync::{RwLock as StdRwLock, Weak};

const ID_LEN: usize = 21;

#[derive(Default)]
pub(super) struct TaskAgentExecutor {
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

        if let Some(lineage) = processor
            .crud_store
            .list_thread_lineage_for_run(run.id.as_str())
            .await?
            .into_iter()
            .last()
        {
            return self
                .recover_existing_child_turn(
                    &processor,
                    &task_response,
                    &run,
                    &agent_spec,
                    lineage,
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
        self.start_new_child_turn(
            &processor,
            &context,
            &task_response.task,
            &run,
            &agent_spec,
            &parent,
            handle,
        )
        .await
    }

    async fn start_new_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        context: &TaskExecutionContext,
        task: &Task,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
        parent: &TaskParentRuntimeContext,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        let child_thread_id = generate_id(ID_LEN);
        let child_turn_id = generate_id(ID_LEN);
        let effective_model = effective_agent_model(agent_spec)?;
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

        let prompt =
            materialize_child_task_prompt(processor, task, run, agent_spec, parent).await?;
        let child_input = vec![UserInput::Text {
            text: prompt,
            text_elements: Vec::new(),
        }];
        let turn_outcome = processor
            .thread_manager
            .system_turn_start(TurnStartParams {
                thread_id: child_thread_id.clone(),
                turn_id: child_turn_id.clone(),
                input: child_input,
                model: Some(effective_model.model),
                model_provider: Some(effective_model.model_provider),
                sandbox_policy: None,
                mode: Some(ThreadMode::Agent),
            })
            .await
            .context("failed to create hidden task turn")?;

        if let Err(error) = processor
            .crud_store
            .materialize_turn_start(
                &turn_outcome.materialization.thread,
                turn_outcome.materialization.sandbox_mode,
                &turn_outcome.materialization.turn,
                &turn_outcome.materialization.input,
            )
            .await
        {
            processor
                .thread_manager
                .rollback_turn_start(turn_outcome.rollback_context)
                .await;
            return Err(error).context("failed to persist hidden task turn");
        }

        let now = now_timestamp_secs();
        handle
            .link_child_thread(
                ThreadLineage {
                    child_thread_id: child_thread_id.clone(),
                    child_turn_id: child_turn_id.clone(),
                    parent_thread_id: parent.parent_thread_id.clone(),
                    parent_turn_id: parent.parent_turn_id.clone(),
                    task_id: task.id.clone(),
                    task_run_id: run.id.clone(),
                    root_thread_id: parent.root_thread_id.clone(),
                    depth: agent_spec.depth,
                    created_at: now,
                },
                now,
            )
            .await?;

        processor
            .agent_manager
            .ensure_thread(child_thread_id.as_str(), context.workspace_id.as_str())
            .await
            .map_err(|error| anyhow!("failed to prepare child agent runtime: {error}"))?;
        processor
            .ensure_agent_listener_task(child_thread_id.as_str())
            .await;

        handle.mark_started(now_timestamp_secs()).await?;
        let workspace_skill_policies =
            load_workspace_skill_policies(processor, task.workspace_id.as_str()).await;
        if let Err(error) = processor
            .agent_manager
            .start_turn(
                child_thread_id.as_str(),
                child_turn_id.as_str(),
                ThreadMode::Agent,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                workspace_skill_policies,
                turn_outcome.materialization.input,
                Vec::new(),
            )
            .await
        {
            processor
                .mark_turn_failed(
                    child_thread_id,
                    child_turn_id,
                    format!("failed to dispatch child task turn: {error}"),
                )
                .await;
            return Ok(TaskExecutorStartOutcome::Started);
        }

        Ok(TaskExecutorStartOutcome::Started)
    }

    async fn recover_existing_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        task_response: &TaskGetResponse,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
        lineage: ThreadLineage,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        let Some((_, turn)) = processor
            .crud_store
            .get_turn(
                lineage.child_thread_id.as_str(),
                lineage.child_turn_id.as_str(),
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
                self.complete_child_turn(processor, &lineage, handle)
                    .await?;
                Ok(TaskExecutorStartOutcome::Started)
            }
            TurnStatus::Failed | TurnStatus::Interrupted => {
                let error_message = turn.error.unwrap_or_else(|| "child turn failed".to_owned());
                self.fail_child_turn(&lineage, error_message.as_str(), handle)
                    .await?;
                Ok(TaskExecutorStartOutcome::Started)
            }
            TurnStatus::InProgress => {
                self.restart_in_progress_child_turn(
                    processor,
                    &task_response.task,
                    run,
                    agent_spec,
                    &lineage,
                    handle,
                )
                .await
            }
        }
    }

    async fn restart_in_progress_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        task: &Task,
        run: &TaskRun,
        agent_spec: &TaskAgentSpec,
        lineage: &ThreadLineage,
        handle: TaskExecutionHandle,
    ) -> Result<TaskExecutorStartOutcome> {
        match self
            .acquire_write_locks(processor, task, run, handle.clone())
            .await?
        {
            TaskExecutorStartOutcome::Started => {}
            outcome => return Ok(outcome),
        }
        let Some(seed_thread) = processor
            .crud_store
            .get_thread_model(lineage.child_thread_id.as_str())
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
            .get_thread_sandbox_mode(lineage.child_thread_id.as_str())
            .await?;
        let parent = resolve_parent_context(processor, task).await?;
        let effective_model = effective_agent_model(agent_spec)?;
        let thread_outcome = processor
            .thread_manager
            .system_thread_start_seeded(
                task.workspace_id.clone(),
                pioneer_protocol::ThreadStartParams {
                    thread_id: lineage.child_thread_id.clone(),
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
        let input = vec![UserInput::Text {
            text: materialize_child_task_prompt(processor, task, run, agent_spec, &parent).await?,
            text_elements: Vec::new(),
        }];
        let turn_outcome = match processor
            .thread_manager
            .system_turn_start(TurnStartParams {
                thread_id: lineage.child_thread_id.clone(),
                turn_id: lineage.child_turn_id.clone(),
                input,
                model: Some(effective_model.model),
                model_provider: Some(effective_model.model_provider),
                sandbox_policy: None,
                mode: Some(ThreadMode::Agent),
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) if format!("{error:#}").contains("already has a running turn") => {
                processor
                    .ensure_agent_listener_task(lineage.child_thread_id.as_str())
                    .await;
                return Ok(TaskExecutorStartOutcome::Queued);
            }
            Err(error) => return Err(error).context("failed to restore hidden task turn"),
        };

        processor
            .agent_manager
            .ensure_thread(lineage.child_thread_id.as_str(), task.workspace_id.as_str())
            .await
            .map_err(|error| anyhow!("failed to restore child agent runtime: {error}"))?;
        processor
            .ensure_agent_listener_task(lineage.child_thread_id.as_str())
            .await;

        if run.status != TaskRunStatus::Running {
            handle.mark_started(now_timestamp_secs()).await?;
        }
        let workspace_skill_policies =
            load_workspace_skill_policies(processor, task.workspace_id.as_str()).await;
        processor
            .agent_manager
            .start_turn(
                lineage.child_thread_id.as_str(),
                lineage.child_turn_id.as_str(),
                ThreadMode::Agent,
                &thread_outcome.started_notification.thread.model,
                &thread_outcome.started_notification.thread.model_provider,
                workspace_skill_policies,
                turn_outcome.materialization.input,
                Vec::new(),
            )
            .await
            .map_err(|error| anyhow!("failed to redispatch child task turn: {error}"))?;

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
        let Some(lineage) = processor.crud_store.get_thread_lineage(thread_id).await? else {
            return Ok(false);
        };
        if lineage.child_turn_id != turn_id {
            return Ok(false);
        }
        let Some(task_response) = processor
            .crud_store
            .get_task(lineage.task_id.as_str())
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
            lineage.task_id.clone(),
            lineage.task_run_id.clone(),
        );
        self.complete_child_turn(&processor, &lineage, handle)
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
        let Some(lineage) = processor.crud_store.get_thread_lineage(thread_id).await? else {
            return Ok(false);
        };
        if lineage.child_turn_id != turn_id {
            return Ok(false);
        }
        let Some(task_response) = processor
            .crud_store
            .get_task(lineage.task_id.as_str())
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
            lineage.task_id.clone(),
            lineage.task_run_id.clone(),
        );
        self.fail_child_turn(&lineage, error_message, handle)
            .await?;
        Ok(true)
    }

    async fn complete_child_turn(
        &self,
        processor: &Arc<MessageProcessor>,
        lineage: &ThreadLineage,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        match TaskAgentResultExtractor::extract(processor, lineage).await? {
            Ok(result) => {
                handle
                    .complete_run(Some(result), now_timestamp_secs())
                    .await?;
            }
            Err(error) => {
                handle.fail_run(Some(error), now_timestamp_secs()).await?;
            }
        }
        Ok(())
    }

    async fn fail_child_turn(
        &self,
        lineage: &ThreadLineage,
        error_message: &str,
        handle: TaskExecutionHandle,
    ) -> Result<()> {
        handle
            .fail_run(
                Some(task_error(
                    "child_turn_failed",
                    error_message.to_owned(),
                    TaskErrorClass::Unknown,
                    Some(lineage.task_run_id.clone()),
                )),
                now_timestamp_secs(),
            )
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
        _handle: TaskExecutionHandle,
    ) -> pioneer_tasks::TaskRuntimeResult<()> {
        let processor = self.processor()?;
        for lineage in processor
            .crud_store
            .list_thread_lineage_for_run(run_id)
            .await?
        {
            let _ = processor
                .agent_manager
                .cancel_turn(
                    lineage.child_thread_id.as_str(),
                    lineage.child_turn_id.as_str(),
                    reason,
                )
                .await;
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
struct TaskParentRuntimeContext {
    parent_thread_id: String,
    parent_turn_id: Option<String>,
    root_thread_id: String,
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
        && let Some(parent_lineage) = processor
            .crud_store
            .list_thread_lineage_for_task(parent_task_id)
            .await?
            .into_iter()
            .last()
    {
        root_thread_id = parent_lineage.root_thread_id;
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

async fn materialize_child_task_prompt(
    processor: &Arc<MessageProcessor>,
    task: &Task,
    run: &TaskRun,
    agent_spec: &TaskAgentSpec,
    parent: &TaskParentRuntimeContext,
) -> Result<String> {
    let mut sections = Vec::new();
    sections.push(format!(
        "You are executing a delegated task.\nTask id: {}\nRun id: {}\nDepth: {}/{}",
        task.id, run.id, agent_spec.depth, agent_spec.max_depth
    ));
    sections.push(render_agent_prompt(&agent_spec.prompt));

    if let Some(context) = render_context_policy(processor, agent_spec, parent).await? {
        sections.push(context);
    }
    if let Some(tool_policy) = agent_spec.tool_policy.as_ref() {
        sections.push(format!(
            "Tool policy:\n- write mode: {:?}\n- network access: {}\n- allowed tools: {}\n- denied tools: {}\n- allowed paths: {}",
            tool_policy.write_mode,
            tool_policy.network_access,
            render_list(&tool_policy.allowed_tools),
            render_list(&tool_policy.denied_tools),
            render_list(&tool_policy.allowed_paths)
        ));
    }
    if let Some(contract) = agent_spec.result_contract.as_ref() {
        let schema = contract
            .schema
            .as_ref()
            .and_then(|schema| serde_json::to_string(&schema.schema).ok())
            .unwrap_or_else(|| "none".to_owned());
        sections.push(format!(
            "Result contract:\n- format: {:?}\n- required: {}\n- schema: {}",
            contract.format, contract.required, schema
        ));
    }

    Ok(sections.join("\n\n"))
}

fn render_agent_prompt(prompt: &TaskAgentPrompt) -> String {
    let mut lines = vec![format!("Goal:\n{}", prompt.goal)];
    if !prompt.instructions.is_empty() {
        lines.push(format!(
            "Instructions:\n{}",
            prompt
                .instructions
                .iter()
                .map(|item| format!("- {item}"))
                .collect::<Vec<_>>()
                .join("\n")
        ));
    }
    if let Some(input) = prompt.input.as_ref()
        && let Some(rendered) = render_agent_input(input)
    {
        lines.push(format!("Input:\n{rendered}"));
    }
    if let Some(output) = prompt.output_instructions.as_deref()
        && !output.trim().is_empty()
    {
        lines.push(format!("Output instructions:\n{output}"));
    }
    lines.join("\n\n")
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
    agent_spec: &TaskAgentSpec,
    parent: &TaskParentRuntimeContext,
) -> Result<Option<String>> {
    let Some(policy) = agent_spec.context_policy.as_ref() else {
        return render_parent_history(processor, parent, Some(6), true).await;
    };

    match policy.mode {
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
    }
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
    let entries = processor
        .crud_store
        .get_thread_conversation_history(parent.parent_thread_id.as_str(), max_turns)
        .await
        .unwrap_or_default();
    if !entries.is_empty() {
        let mut lines = Vec::new();
        for entry in entries {
            if let Some(user_text) = entry.user_text {
                lines.push(format!("User: {user_text}"));
            }
            if let Some(assistant_text) = entry.assistant_text {
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

struct StructuredResultCandidate {
    value: TaskValue,
}

impl TaskAgentResultExtractor {
    async fn extract(
        processor: &Arc<MessageProcessor>,
        lineage: &ThreadLineage,
    ) -> Result<TaskAgentResultExtraction> {
        let contract = match processor
            .crud_store
            .get_task(lineage.task_id.as_str())
            .await?
        {
            Some(response) => select_agent_spec(&response, lineage.task_run_id.as_str())
                .and_then(|spec| spec.result_contract),
            None => None,
        };

        let messages = processor
            .crud_store
            .list_completed_agent_messages(lineage.child_turn_id.as_str())
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
                Some(lineage.task_run_id.clone()),
            )));
        };

        Ok(Self::normalize_final_message(
            raw_text,
            source_item_id,
            lineage,
            contract.as_ref(),
        ))
    }

    fn normalize_final_message(
        raw_text: String,
        source_item_id: String,
        lineage: &ThreadLineage,
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
                    lineage,
                ));
            }
            diagnostics.extend(schema_errors);
        }

        Ok(fallback_text_task_result(
            raw_text.as_str(),
            source_item_id,
            lineage,
            diagnostics,
        ))
    }
}

fn task_result_from_structured_candidate(
    candidate: StructuredResultCandidate,
    raw_text: &str,
    lineage: &ThreadLineage,
) -> TaskResult {
    let (summary, data, artifacts) = match candidate.value {
        TaskValue::Object(mut object) => {
            let artifacts = object
                .remove("artifacts")
                .and_then(|value| match value {
                    TaskValue::List(values) => Some(parse_task_artifacts(&values)),
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
        completed_by_run_id: Some(lineage.task_run_id.clone()),
    };
    if let Some(TaskValue::Object(object)) = &result.data
        && let Some(TaskValue::List(artifact_values)) = object.get("artifacts")
    {
        result.artifacts = parse_task_artifacts(artifact_values);
    }
    result
}

fn fallback_text_task_result(
    raw_text: &str,
    source_item_id: String,
    lineage: &ThreadLineage,
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
            TaskValue::String(lineage.child_thread_id.clone()),
        ),
        (
            "sourceTurnId".to_owned(),
            TaskValue::String(lineage.child_turn_id.clone()),
        ),
        ("sourceItemId".to_owned(), TaskValue::String(source_item_id)),
    ]));
    TaskResult {
        summary: first_meaningful_line(fallback_text.as_str()),
        data: Some(data),
        artifacts: Vec::new(),
        completed_by_run_id: Some(lineage.task_run_id.clone()),
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

fn parse_task_artifacts(values: &[TaskValue]) -> Vec<TaskArtifact> {
    values
        .iter()
        .filter_map(|value| {
            let object = task_value_object(value)?;
            Some(TaskArtifact {
                artifact_id: object
                    .get("artifactId")
                    .or_else(|| object.get("artifact_id"))
                    .and_then(task_value_str)
                    .map(str::to_owned),
                path: object
                    .get("path")
                    .and_then(task_value_str)
                    .map(str::to_owned),
                url: object
                    .get("url")
                    .and_then(task_value_str)
                    .map(str::to_owned),
                mime_type: object
                    .get("mimeType")
                    .or_else(|| object.get("mime_type"))
                    .and_then(task_value_str)
                    .map(str::to_owned),
                metadata: object.get("metadata").cloned(),
            })
        })
        .collect()
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

fn render_list(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_owned()
    } else {
        values.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_lineage() -> ThreadLineage {
        ThreadLineage {
            child_thread_id: "child_thread".to_owned(),
            child_turn_id: "child_turn".to_owned(),
            parent_thread_id: "parent_thread".to_owned(),
            parent_turn_id: Some("parent_turn".to_owned()),
            task_id: "task".to_owned(),
            task_run_id: "run".to_owned(),
            root_thread_id: "parent_thread".to_owned(),
            depth: 1,
            created_at: 1,
        }
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
            &test_lineage(),
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
    fn result_extractor_falls_back_when_structured_block_violates_schema() {
        let raw_text = r#"Plain fallback result.
```task-result
{"summary":"Bad structured summary","data":{"answer":42}}
```"#;

        let result = TaskAgentResultExtractor::normalize_final_message(
            raw_text.to_owned(),
            "item".to_owned(),
            &test_lineage(),
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
