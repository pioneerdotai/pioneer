use crate::message::{MessageProcessor, now_timestamp_secs};
use async_trait::async_trait;
use pioneer_agent::{
    PendingAttachedTask, TaskToolMaterialization, TaskToolProvider, TaskTurnContext,
    TerminalTaskObservation,
};
use pioneer_protocol::{
    ItemCompletedNotification, ItemUpdatedNotification, Task, TaskAgentContextPolicy,
    TaskAgentInput, TaskAgentPrompt, TaskAgentResultContract, TaskAgentSpec, TaskAgentSpecInput,
    TaskAttachmentMode, TaskCancelParams, TaskCompletionBehavior, TaskCreateParams,
    TaskCreateResponse, TaskDeliveryPolicy, TaskDetachParams, TaskError, TaskExecutorKind,
    TaskGetParams, TaskGetResponse, TaskLifecyclePolicy, TaskListParams, TaskMetadata,
    TaskOwnerKind, TaskParentTerminalAction, TaskPauseParams, TaskRescheduleParams, TaskResult,
    TaskResumeParams, TaskRetryPolicy, TaskRun, TaskRunStatus, TaskStatus, TaskTimeoutPolicy,
    TaskTrigger, TaskTriggerInput, TaskTriggerKind, TaskTriggerSpec, TaskTurnItem, ToolCallStatus,
    ToolStoragePayload, TurnItem, TurnItemEventPayload, constants::events,
};
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, FunctionToolOutput, PayloadKind, ToolError,
    ToolExtensionBundle, ToolHandler, ToolIdempotencyMode, ToolInvocation, ToolOutput, ToolPayload,
    ToolRecoveryMetadata, ToolRetryClass, ToolSpec, dynamic_unknown_output_policy,
};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeSet;
use std::sync::{Arc, Weak};

const TASK_CREATE_TOOL: &str = "task_create";
const TASK_WAIT_TOOL: &str = "task_wait";
const TASK_CANCEL_TOOL: &str = "task_cancel";
const TASK_DETACH_TOOL: &str = "task_detach";
const TASK_LIST_TOOL: &str = "task_list";
const TASK_GET_TOOL: &str = "task_get";
const TASK_RESCHEDULE_TOOL: &str = "task_reschedule";
const TASK_PAUSE_TOOL: &str = "task_pause";
const TASK_RESUME_TOOL: &str = "task_resume";
const DEFAULT_ROOT_MAX_DEPTH: i64 = 3;
const DEFAULT_TASK_LIST_LIMIT: u32 = 20;
const GUARD_TASK_LIST_LIMIT: u32 = 500;

#[derive(Clone)]
pub(crate) struct GatewayTaskToolProvider {
    processor: Weak<MessageProcessor>,
}

impl GatewayTaskToolProvider {
    pub(crate) fn new(processor: Weak<MessageProcessor>) -> Self {
        Self { processor }
    }

    fn processor(&self) -> Result<Arc<MessageProcessor>, String> {
        self.processor
            .upgrade()
            .ok_or_else(|| "message processor is no longer available".to_owned())
    }
}

#[async_trait]
impl TaskToolProvider for GatewayTaskToolProvider {
    async fn materialize_task_tools(
        &self,
        context: TaskTurnContext,
    ) -> Result<TaskToolMaterialization, String> {
        let processor = self.processor()?;
        let handler = Arc::new(TaskToolHandler { processor, context });
        let mut bundle = ToolExtensionBundle::default();
        for configured in task_tool_specs() {
            let name = configured.spec.name.clone();
            bundle.specs.push(configured);
            bundle.handlers.push((name, handler.clone()));
        }
        Ok(TaskToolMaterialization {
            bundles: vec![bundle],
            diagnostics: Vec::new(),
        })
    }

    async fn pending_attached_tasks(
        &self,
        context: TaskTurnContext,
    ) -> Result<Vec<PendingAttachedTask>, String> {
        let processor = self.processor()?;
        let tasks = attached_tasks_for_turn(&processor, &context)
            .await
            .map_err(|error| format!("{error:#}"))?;
        let mut pending = Vec::new();
        for task in tasks {
            if task.status.is_terminal() {
                continue;
            }
            let run_id = processor
                .crud_store
                .get_task(task.id.as_str())
                .await
                .map_err(|error| format!("{error:#}"))?
                .and_then(|response| response.runs.last().map(|run| run.id.clone()));
            pending.push(PendingAttachedTask {
                task_id: task.id,
                run_id,
                title: task.title,
                status: task_status_label(task.status),
            });
        }
        Ok(pending)
    }

    async fn terminal_attached_task_observations(
        &self,
        context: TaskTurnContext,
    ) -> Result<Vec<TerminalTaskObservation>, String> {
        let processor = self.processor()?;
        let observed = observed_terminal_task_ids(&processor, &context)
            .await
            .map_err(|error| format!("{error:#}"))?;
        let tasks = attached_tasks_for_turn(&processor, &context)
            .await
            .map_err(|error| format!("{error:#}"))?;
        let mut observations = Vec::new();
        for task in tasks {
            if !task.status.is_terminal() || observed.contains(task.id.as_str()) {
                continue;
            }
            let response = processor
                .crud_store
                .get_task(task.id.as_str())
                .await
                .map_err(|error| format!("{error:#}"))?
                .ok_or_else(|| format!("task `{}` disappeared", task.id))?;
            let run = response.runs.last();
            let lineage = match run {
                Some(run) => processor
                    .crud_store
                    .list_thread_lineage_for_run(run.id.as_str())
                    .await
                    .map_err(|error| format!("{error:#}"))?
                    .into_iter()
                    .last(),
                None => None,
            };
            observations.push(TerminalTaskObservation {
                task_id: response.task.id.clone(),
                run_id: run.map(|run| run.id.clone()),
                title: response.task.title.clone(),
                status: task_status_label(response.task.status),
                summary: run
                    .and_then(|run| run.result.as_ref())
                    .and_then(|result| result.summary.clone())
                    .or_else(|| {
                        response
                            .task
                            .result
                            .as_ref()
                            .and_then(|result| result.summary.clone())
                    }),
                error_message: run
                    .and_then(|run| run.error.as_ref())
                    .map(|error| error.message.clone())
                    .or_else(|| {
                        response
                            .task
                            .error
                            .as_ref()
                            .map(|error| error.message.clone())
                    }),
                child_thread_id: lineage
                    .as_ref()
                    .map(|lineage| lineage.child_thread_id.clone()),
                child_turn_id: lineage
                    .as_ref()
                    .map(|lineage| lineage.child_turn_id.clone()),
            });
        }
        Ok(observations)
    }

    async fn cleanup_attached_tasks(
        &self,
        context: TaskTurnContext,
        reason: String,
    ) -> Result<(), String> {
        let processor = self.processor()?;
        let tasks = attached_tasks_for_turn(&processor, &context)
            .await
            .map_err(|error| format!("{error:#}"))?;
        for task in tasks {
            if task.status.is_terminal() {
                continue;
            }
            processor
                .task_runtime
                .service()
                .cancel_task(
                    pioneer_tasks::TaskMutationContext::default(),
                    TaskCancelParams {
                        task_id: task.id,
                        reason: Some(reason.clone()),
                        scope: pioneer_protocol::TaskCancelScope::AttachedSubtree,
                    },
                )
                .await
                .map_err(|error| format!("{error:#}"))?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct TaskToolHandler {
    processor: Arc<MessageProcessor>,
    context: TaskTurnContext,
}

#[async_trait]
impl ToolHandler for TaskToolHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: pioneer_tools::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        match invocation.tool_name.as_str() {
            TASK_CREATE_TOOL => self.handle_create(invocation).await,
            TASK_WAIT_TOOL => self.handle_wait(invocation).await,
            TASK_CANCEL_TOOL => self.handle_cancel(invocation).await,
            TASK_DETACH_TOOL => self.handle_detach(invocation).await,
            TASK_LIST_TOOL => self.handle_list(invocation).await,
            TASK_GET_TOOL => self.handle_get(invocation).await,
            TASK_RESCHEDULE_TOOL => self.handle_reschedule(invocation).await,
            TASK_PAUSE_TOOL => self.handle_pause(invocation).await,
            TASK_RESUME_TOOL => self.handle_resume(invocation).await,
            other => Err(ToolError::NotFound(other.to_owned())),
        }
    }
}

impl TaskToolHandler {
    async fn handle_create(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: TaskCreateToolInput = decode_tool_args(invocation)?;
        let params = self.create_params(input).await?;
        let response = self
            .processor
            .task_runtime
            .service()
            .create_task(pioneer_tasks::TaskCreateContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let anchor = self.persist_task_anchor(response.task.id.as_str()).await?;
        let output = task_create_tool_output(&response, &anchor);
        Ok(function_output(output))
    }

    async fn handle_wait(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let current_call_id = invocation.call_id.clone();
        let mut params: pioneer_protocol::TaskWaitParams = decode_tool_args(invocation)?;
        if !params.return_completed && !params.return_pending {
            params.return_completed = true;
            params.return_pending = true;
        }
        let signature = TaskWaitSignature::from_params(&params);
        if let Some(guard_output) = self
            .duplicate_wait_guard(signature.clone(), &params, current_call_id.as_str())
            .await?
        {
            return Ok(function_output(guard_output));
        }
        let response = self
            .processor
            .task_runtime
            .service()
            .wait_tasks(pioneer_tasks::TaskWaitContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        Ok(function_output(task_wait_tool_output(
            &response, &signature,
        )))
    }

    async fn duplicate_wait_guard(
        &self,
        signature: TaskWaitSignature,
        params: &pioneer_protocol::TaskWaitParams,
        current_call_id: &str,
    ) -> Result<Option<JsonValue>, ToolError> {
        let prior = prior_wait_calls_for_signature(
            &self.processor,
            &self.context,
            &signature,
            current_call_id,
        )
        .await
        .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        if prior.is_empty() {
            return Ok(None);
        }

        let mut state_params = params.clone();
        state_params.timeout_ms = Some(0);
        state_params.return_completed = false;
        state_params.return_pending = false;
        let state = self
            .processor
            .task_runtime
            .service()
            .wait_tasks(pioneer_tasks::TaskWaitContext::default(), state_params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;

        if !duplicate_wait_should_block(&prior, state.terminal_count, state.pending_count) {
            return Ok(None);
        }
        let last = prior
            .last()
            .expect("prior wait list checked as non-empty before last()");

        Ok(Some(task_wait_guard_output(
            &signature,
            last.item_id.as_str(),
            state.total_count,
            state.terminal_count,
            state.pending_count,
            last.timed_out,
            prior.len(),
        )))
    }

    async fn handle_cancel(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let params: TaskCancelParams = decode_tool_args(invocation)?;
        let response = self
            .processor
            .task_runtime
            .service()
            .cancel_task(pioneer_tasks::TaskMutationContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        Ok(function_output(
            json!({ "task": task_summary(&response.task) }),
        ))
    }

    async fn handle_detach(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let params: TaskDetachParams = decode_tool_args(invocation)?;
        let response = self
            .processor
            .task_runtime
            .service()
            .detach_task(pioneer_tasks::TaskMutationContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        Ok(function_output(
            json!({ "task": task_summary(&response.task) }),
        ))
    }

    async fn handle_list(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: TaskListToolInput = decode_tool_args(invocation)?;
        let params = TaskListParams {
            workspace_id: self.context.workspace_id.clone(),
            owner_kind: input.owner_kind,
            owner_id: input.owner_id,
            parent_task_id: input.parent_task_id,
            root_task_id: input.root_task_id,
            status: input.status,
            limit: Some(
                input
                    .limit
                    .unwrap_or(DEFAULT_TASK_LIST_LIMIT)
                    .max(1)
                    .min(100),
            ),
        };
        let response = self
            .processor
            .task_runtime
            .service()
            .list_tasks(params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let tasks = response.tasks.iter().map(task_summary).collect::<Vec<_>>();
        Ok(function_output(json!({ "tasks": tasks })))
    }

    async fn handle_get(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let params: TaskGetParams = decode_tool_args(invocation)?;
        let response = self
            .processor
            .task_runtime
            .service()
            .get_task(params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let mut lineages = Vec::new();
        for run in &response.runs {
            let mut run_lineages = self
                .processor
                .crud_store
                .list_thread_lineage_for_run(run.id.as_str())
                .await
                .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
            lineages.append(&mut run_lineages);
        }
        let payload = json!({
            "task": response.task,
            "triggers": response.triggers,
            "runs": response.runs,
            "agentSpecs": response.agent_specs,
            "dependencies": response.dependencies,
            "lineage": lineages,
        });
        Ok(function_output(payload))
    }

    async fn handle_reschedule(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let params: TaskRescheduleParams = decode_tool_args(invocation)?;
        let response = self
            .processor
            .task_runtime
            .service()
            .reschedule_task(pioneer_tasks::TaskMutationContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        Ok(function_output(json!({
            "task": task_summary(&response.task),
            "trigger": response.trigger,
        })))
    }

    async fn handle_pause(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let params: TaskPauseParams = decode_tool_args(invocation)?;
        let response = self
            .processor
            .task_runtime
            .service()
            .pause_task(pioneer_tasks::TaskMutationContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        Ok(function_output(json!({
            "task": task_summary(&response.task),
            "triggers": response.triggers,
        })))
    }

    async fn handle_resume(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let params: TaskResumeParams = decode_tool_args(invocation)?;
        let response = self
            .processor
            .task_runtime
            .service()
            .resume_task(pioneer_tasks::TaskMutationContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        Ok(function_output(json!({
            "task": task_summary(&response.task),
            "triggers": response.triggers,
        })))
    }

    async fn create_params(
        &self,
        input: TaskCreateToolInput,
    ) -> Result<TaskCreateParams, ToolError> {
        let title = required_tool_string(input.title.as_deref(), "title")?;
        let goal = required_tool_string(input.goal.as_deref(), "goal")?;
        let (model, model_provider) =
            current_thread_model_identity(&self.processor, &self.context).await?;
        let trigger = input.trigger.unwrap_or(TaskTriggerInput {
            spec: TaskTriggerSpec::Immediate,
        });
        let executor_kind = input.executor_kind.unwrap_or(TaskExecutorKind::Agent);
        let parent_task_id = current_parent_task_id(&self.processor, &self.context)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let inherited_max_depth =
            inherited_max_depth(&self.processor, parent_task_id.as_deref()).await?;
        let requested_max_depth = input.max_depth.unwrap_or(inherited_max_depth);
        let instructions = if input.instructions.is_empty() {
            vec!["Return a concise final result.".to_owned()]
        } else {
            input.instructions
        };
        let prompt = TaskAgentPrompt {
            goal: goal.clone(),
            instructions,
            input: input.input.or_else(|| {
                input.input_text.map(|text| TaskAgentInput {
                    text: Some(text),
                    variables: Vec::new(),
                    attachments: Vec::new(),
                    references: Vec::new(),
                })
            }),
            output_instructions: input.output_instructions,
        };
        let agent_spec = (executor_kind == TaskExecutorKind::Agent).then_some(TaskAgentSpecInput {
            agent_role: input.agent_role,
            agent_nickname: input.agent_nickname,
            model: Some(model),
            model_provider: Some(model_provider),
            prompt,
            context_policy: input.context_policy,
            tool_policy: input.tool_policy,
            result_contract: input.result_contract,
            depth: 0,
            max_depth: requested_max_depth,
        });
        Ok(TaskCreateParams {
            workspace_id: self.context.workspace_id.clone(),
            owner_kind: TaskOwnerKind::Thread,
            owner_id: Some(self.context.thread_id.clone()),
            created_by_thread_id: Some(self.context.thread_id.clone()),
            created_by_turn_id: Some(self.context.turn_id.clone()),
            parent_task_id,
            executor_kind,
            title,
            goal,
            priority: input.priority.unwrap_or_default(),
            trigger,
            agent_spec,
            lifecycle_policy: input.lifecycle_policy,
            delivery_policy: input.delivery_policy,
            retry_policy: input.retry_policy,
            timeout_policy: input.timeout_policy,
            concurrency_policy: input.concurrency_policy,
            metadata: input.metadata,
        })
    }

    async fn persist_task_anchor(&self, task_id: &str) -> Result<TaskTurnItem, ToolError> {
        let task_response = self
            .processor
            .crud_store
            .get_task(task_id)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?
            .ok_or_else(|| ToolError::execution_failed("created task was not found"))?;
        let item = task_turn_item_from_response(&self.processor, &task_response)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let notification = ItemCompletedNotification {
            workspace_id: self.context.workspace_id.clone(),
            thread_id: self.context.thread_id.clone(),
            turn_id: self.context.turn_id.clone(),
            item: TurnItem::Task { item: item.clone() },
        };
        let now = now_timestamp_secs();
        self.processor
            .crud_store
            .materialize_item_completed(notification.clone(), now)
            .await
            .map_err(|error| {
                ToolError::execution_failed(format!(
                    "task was created but anchor persistence failed: {error:#}"
                ))
            })?;
        self.processor
            .send_notification_to_thread_subscribers(
                self.context.thread_id.as_str(),
                events::ITEM_COMPLETED,
                &notification,
            )
            .await;
        let latest_response = self
            .processor
            .crud_store
            .get_task(task_id)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?
            .ok_or_else(|| ToolError::execution_failed("created task was not found"))?;
        let latest_item = task_turn_item_from_response(&self.processor, &latest_response)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        if latest_item != item {
            let update = ItemUpdatedNotification {
                workspace_id: self.context.workspace_id.clone(),
                thread_id: self.context.thread_id.clone(),
                turn_id: self.context.turn_id.clone(),
                item: TurnItem::Task {
                    item: latest_item.clone(),
                },
            };
            self.processor
                .crud_store
                .materialize_item_updated(update.clone(), now)
                .await
                .map_err(|error| {
                    ToolError::execution_failed(format!(
                        "task anchor was created but refresh failed: {error:#}"
                    ))
                })?;
            self.processor
                .send_notification_to_thread_subscribers(
                    self.context.thread_id.as_str(),
                    events::ITEM_UPDATED,
                    &update,
                )
                .await;
        }
        Ok(latest_item)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TaskCreateToolInput {
    title: Option<String>,
    goal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trigger: Option<TaskTriggerInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    executor_kind: Option<TaskExecutorKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    agent_nickname: Option<String>,
    #[serde(default)]
    instructions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input: Option<TaskAgentInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    output_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context_policy: Option<TaskAgentContextPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tool_policy: Option<pioneer_protocol::TaskAgentToolPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_contract: Option<TaskAgentResultContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_depth: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    priority: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lifecycle_policy: Option<TaskLifecyclePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    delivery_policy: Option<TaskDeliveryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retry_policy: Option<TaskRetryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timeout_policy: Option<TaskTimeoutPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    concurrency_policy: Option<pioneer_protocol::TaskConcurrencyPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<TaskMetadata>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TaskListToolInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner_kind: Option<TaskOwnerKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    root_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status: Option<TaskStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
}

fn task_tool_specs() -> Vec<ConfiguredToolSpec> {
    vec![
        task_tool_spec(
            TASK_CREATE_TOOL,
            "Create a durable task. Immediate agent tasks are attached by default; scheduled, interval, and cron tasks are detached by default. Parent/root/depth context is derived by runtime and must not be supplied.",
            task_create_schema(),
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::RequiresKey,
                max_attempts: 1,
                can_resume: false,
            },
        ),
        task_tool_spec(
            TASK_WAIT_TOOL,
            "Join one or more task ids or run ids using the task event bus. Use once for attached tasks; it blocks until all targets are terminal by default or timeout. Do not repeatedly call it for the same task set unless the prior wait timed out.",
            task_wait_schema(),
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Transient,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 2,
                can_resume: true,
            },
        ),
        task_tool_spec(
            TASK_CANCEL_TOOL,
            "Cancel a task through the task service.",
            task_cancel_schema(),
            safe_mutation_recovery(),
        ),
        task_tool_spec(
            TASK_DETACH_TOOL,
            "Detach a task from the current parent turn so it no longer blocks parent completion.",
            task_id_schema(),
            safe_mutation_recovery(),
        ),
        task_tool_spec(
            TASK_LIST_TOOL,
            "List durable tasks in the current workspace. Results are bounded and summarized by default.",
            task_list_schema(),
            safe_read_recovery(),
        ),
        task_tool_spec(
            TASK_GET_TOOL,
            "Get full task details, including runs, triggers, agent specs, dependencies, and child lineage.",
            task_id_schema(),
            safe_read_recovery(),
        ),
        task_tool_spec(
            TASK_RESCHEDULE_TOOL,
            "Reschedule a non-terminal task through the task service.",
            task_reschedule_schema(),
            safe_mutation_recovery(),
        ),
        task_tool_spec(
            TASK_PAUSE_TOOL,
            "Pause a non-terminal task through the task service. Running task runs are not cancelled.",
            task_cancel_schema(),
            safe_mutation_recovery(),
        ),
        task_tool_spec(
            TASK_RESUME_TOOL,
            "Resume a paused task through the task service and recompute its next scheduled fire.",
            task_cancel_schema(),
            safe_mutation_recovery(),
        ),
    ]
}

fn task_tool_spec(
    name: &str,
    description: &str,
    parameters: JsonValue,
    recovery: ToolRecoveryMetadata,
) -> ConfiguredToolSpec {
    ConfiguredToolSpec::with_output_projection(
        ToolSpec::new(name, description, parameters, PayloadKind::Function).with_recovery(recovery),
        ExecutionClass::Shared,
        dynamic_unknown_output_policy(),
        pioneer_tools::ToolOutputProjectionKind::DynamicGeneric,
    )
}

fn safe_read_recovery() -> ToolRecoveryMetadata {
    ToolRecoveryMetadata {
        retry_class: ToolRetryClass::Transient,
        idempotency_mode: ToolIdempotencyMode::Safe,
        max_attempts: 2,
        can_resume: true,
    }
}

fn safe_mutation_recovery() -> ToolRecoveryMetadata {
    ToolRecoveryMetadata {
        retry_class: ToolRetryClass::Transient,
        idempotency_mode: ToolIdempotencyMode::RequiresKey,
        max_attempts: 1,
        can_resume: false,
    }
}

fn task_create_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "title": { "type": "string", "description": "Short task title." },
            "goal": { "type": "string", "description": "Concrete task goal for the executor." },
            "instructions": { "type": "array", "items": { "type": "string" } },
            "inputText": { "type": "string" },
            "trigger": { "type": "object", "description": "TaskTriggerInput. Defaults to immediate." },
            "executorKind": { "type": "string", "enum": ["agent"] },
            "agentRole": { "type": "string" },
            "agentNickname": { "type": "string" },
            "maxDepth": { "type": "integer", "minimum": 0 },
            "priority": { "type": "integer" },
            "contextPolicy": { "type": "object" },
            "toolPolicy": { "type": "object" },
            "resultContract": { "type": "object" },
            "lifecyclePolicy": { "type": "object" },
            "deliveryPolicy": { "type": "object" },
            "retryPolicy": { "type": "object" },
            "timeoutPolicy": { "type": "object" },
            "concurrencyPolicy": { "type": "object" },
            "metadata": { "type": "object" }
        },
        "required": ["title", "goal"],
        "additionalProperties": false
    })
}

fn task_wait_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "taskIds": { "type": "array", "items": { "type": "string" } },
            "runIds": { "type": "array", "items": { "type": "string" } },
            "timeoutMs": { "type": "integer", "minimum": 1 },
            "mode": { "type": "string", "enum": ["all_terminal", "any_terminal"] },
            "returnCompleted": { "type": "boolean" },
            "returnPending": { "type": "boolean" }
        },
        "additionalProperties": false
    })
}

fn task_cancel_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "taskId": { "type": "string" },
            "reason": { "type": "string" }
        },
        "required": ["taskId"],
        "additionalProperties": false
    })
}

fn task_id_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "taskId": { "type": "string" }
        },
        "required": ["taskId"],
        "additionalProperties": false
    })
}

fn task_list_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "ownerKind": { "type": "string", "enum": ["user", "thread", "workspace", "system"] },
            "ownerId": { "type": "string" },
            "parentTaskId": { "type": "string" },
            "rootTaskId": { "type": "string" },
            "status": { "type": "string", "enum": ["draft", "scheduled", "queued", "running", "waiting", "completed", "failed", "cancelled"] },
            "limit": { "type": "integer", "minimum": 1, "maximum": 100 }
        },
        "additionalProperties": false
    })
}

fn task_reschedule_schema() -> JsonValue {
    json!({
        "type": "object",
        "properties": {
            "taskId": { "type": "string" },
            "trigger": { "type": "object", "description": "TaskTriggerInput." }
        },
        "required": ["taskId", "trigger"],
        "additionalProperties": false
    })
}

fn decode_tool_args<T>(invocation: ToolInvocation) -> Result<T, ToolError>
where
    T: for<'de> Deserialize<'de>,
{
    let arguments = match invocation.payload {
        ToolPayload::Function { arguments } => arguments,
        ToolPayload::Custom { input } => serde_json::from_str(&input).map_err(|error| {
            ToolError::invalid_arguments(format!("failed to parse custom task tool input: {error}"))
        })?,
        other => {
            return Err(ToolError::invalid_arguments(format!(
                "task tools require function arguments, got {}",
                other.log_payload()
            )));
        }
    };
    serde_json::from_value(arguments).map_err(|error| {
        ToolError::invalid_arguments(format!("invalid task tool arguments: {error}"))
    })
}

fn function_output(payload: JsonValue) -> Box<dyn ToolOutput> {
    let text = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| payload.to_string());
    Box::new(FunctionToolOutput::with_payload(text, true, payload))
}

fn required_tool_string(value: Option<&str>, field: &str) -> Result<String, ToolError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(ToolError::invalid_arguments(format!(
            "`{field}` is required"
        )));
    };
    Ok(value.to_owned())
}

async fn current_parent_task_id(
    processor: &Arc<MessageProcessor>,
    context: &TaskTurnContext,
) -> anyhow::Result<Option<String>> {
    Ok(processor
        .crud_store
        .get_thread_lineage(context.thread_id.as_str())
        .await?
        .map(|lineage| lineage.task_id))
}

async fn current_thread_model_identity(
    processor: &Arc<MessageProcessor>,
    context: &TaskTurnContext,
) -> Result<(String, String), ToolError> {
    let thread = processor
        .crud_store
        .get_thread_model(context.thread_id.as_str())
        .await
        .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?
        .ok_or_else(|| {
            ToolError::execution_failed(format!(
                "unable to resolve current thread `{}` model identity",
                context.thread_id
            ))
        })?;

    let model = thread.model.trim();
    if model.is_empty() {
        return Err(ToolError::execution_failed(format!(
            "thread `{}` has empty model",
            context.thread_id
        )));
    }

    let model_provider = thread.model_provider.trim();
    if model_provider.is_empty() {
        return Err(ToolError::execution_failed(format!(
            "thread `{}` has empty model_provider",
            context.thread_id
        )));
    }

    Ok((model.to_owned(), model_provider.to_owned()))
}

async fn inherited_max_depth(
    processor: &Arc<MessageProcessor>,
    parent_task_id: Option<&str>,
) -> Result<i64, ToolError> {
    let Some(parent_task_id) = parent_task_id else {
        return Ok(DEFAULT_ROOT_MAX_DEPTH);
    };
    let response = processor
        .crud_store
        .get_task(parent_task_id)
        .await
        .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?
        .ok_or_else(|| {
            ToolError::invalid_arguments(format!("parent task `{parent_task_id}` not found"))
        })?;
    Ok(response
        .agent_specs
        .last()
        .map(|spec| spec.max_depth)
        .unwrap_or(DEFAULT_ROOT_MAX_DEPTH))
}

async fn attached_tasks_for_turn(
    processor: &Arc<MessageProcessor>,
    context: &TaskTurnContext,
) -> anyhow::Result<Vec<Task>> {
    let response = processor
        .task_runtime
        .service()
        .list_tasks(TaskListParams {
            workspace_id: context.workspace_id.clone(),
            owner_kind: Some(TaskOwnerKind::Thread),
            owner_id: Some(context.thread_id.clone()),
            parent_task_id: None,
            root_task_id: None,
            status: None,
            limit: Some(GUARD_TASK_LIST_LIMIT),
        })
        .await?;
    Ok(response
        .tasks
        .into_iter()
        .filter(|task| task.created_by_turn_id.as_deref() == Some(context.turn_id.as_str()))
        .filter(|task| {
            task.lifecycle_policy
                .as_ref()
                .map(|policy| policy.attachment == TaskAttachmentMode::Attached)
                .unwrap_or(false)
        })
        .collect())
}

async fn observed_terminal_task_ids(
    processor: &Arc<MessageProcessor>,
    context: &TaskTurnContext,
) -> anyhow::Result<BTreeSet<String>> {
    let Some(parent_events) = processor
        .crud_store
        .get_turn_item_events(context.thread_id.as_str(), context.turn_id.as_str())
        .await?
    else {
        return Ok(BTreeSet::new());
    };
    let mut task_ids = BTreeSet::new();
    for event in parent_events.events {
        let TurnItemEventPayload::ItemCompleted {
            item:
                TurnItem::SystemEvent {
                    code,
                    details: Some(details),
                    ..
                },
            ..
        } = event.payload
        else {
            continue;
        };
        if code.as_deref() != Some("task.terminal.observed") {
            continue;
        }
        if let Some(ids) = details.get("taskIds").and_then(JsonValue::as_array) {
            for task_id in ids.iter().filter_map(JsonValue::as_str) {
                task_ids.insert(task_id.to_owned());
            }
        }
    }
    Ok(task_ids)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskWaitSignature {
    task_ids: Vec<String>,
    run_ids: Vec<String>,
    mode: pioneer_protocol::TaskWaitMode,
}

impl TaskWaitSignature {
    fn from_params(params: &pioneer_protocol::TaskWaitParams) -> Self {
        let mut task_ids = params.task_ids.clone();
        task_ids.sort();
        task_ids.dedup();
        let mut run_ids = params.run_ids.clone();
        run_ids.sort();
        run_ids.dedup();
        Self {
            task_ids,
            run_ids,
            mode: params.mode,
        }
    }

    fn from_arguments(arguments: &JsonValue) -> Option<Self> {
        let params =
            serde_json::from_value::<pioneer_protocol::TaskWaitParams>(arguments.clone()).ok()?;
        Some(Self::from_params(&params))
    }

    fn to_json(&self) -> JsonValue {
        json!({
            "taskIds": self.task_ids,
            "runIds": self.run_ids,
            "mode": wait_mode_label(self.mode),
        })
    }
}

#[derive(Debug, Clone)]
struct PriorWaitCall {
    item_id: String,
    timed_out: bool,
    terminal_count: u32,
}

async fn prior_wait_calls_for_signature(
    processor: &Arc<MessageProcessor>,
    context: &TaskTurnContext,
    signature: &TaskWaitSignature,
    current_call_id: &str,
) -> anyhow::Result<Vec<PriorWaitCall>> {
    let Some(parent_events) = processor
        .crud_store
        .get_turn_item_events(context.thread_id.as_str(), context.turn_id.as_str())
        .await?
    else {
        return Ok(Vec::new());
    };

    let mut prior_calls = Vec::<PriorWaitCall>::new();
    for event in parent_events.events {
        let item = match event.payload {
            TurnItemEventPayload::ItemCompleted { item, .. }
            | TurnItemEventPayload::ItemUpdated { item, .. } => item,
            _ => continue,
        };
        let Some(prior) = prior_wait_call_from_item(item, signature, current_call_id) else {
            continue;
        };
        if let Some(existing) = prior_calls
            .iter_mut()
            .find(|existing| existing.item_id == prior.item_id)
        {
            *existing = prior;
        } else {
            prior_calls.push(prior);
        }
    }
    Ok(prior_calls)
}

fn prior_wait_call_from_item(
    item: TurnItem,
    signature: &TaskWaitSignature,
    current_call_id: &str,
) -> Option<PriorWaitCall> {
    let TurnItem::DynamicToolCall {
        id,
        tool_name,
        arguments,
        status,
        storage,
        ..
    } = item
    else {
        return None;
    };
    if id == current_call_id || tool_name != TASK_WAIT_TOOL || status != ToolCallStatus::Completed {
        return None;
    }
    if TaskWaitSignature::from_arguments(&arguments).as_ref() != Some(signature) {
        return None;
    }
    let wait_result = wait_result_from_storage(&storage)?;
    Some(PriorWaitCall {
        item_id: id,
        timed_out: wait_result
            .get("timedOut")
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
        terminal_count: json_u32(&wait_result, "terminalCount"),
    })
}

fn duplicate_wait_should_block(
    prior: &[PriorWaitCall],
    terminal_count: u32,
    pending_count: u32,
) -> bool {
    if prior.is_empty() || pending_count == 0 {
        return false;
    }
    let last = prior
        .last()
        .expect("prior wait list checked as non-empty before last()");
    if terminal_count > last.terminal_count {
        return false;
    }
    if last.timed_out {
        return prior.iter().filter(|entry| entry.timed_out).count() >= 2;
    }
    true
}

fn wait_result_from_storage(storage: &ToolStoragePayload) -> Option<JsonValue> {
    match storage {
        ToolStoragePayload::Metadata { metadata } => {
            metadata.to_json().get("sanitizedResult").cloned()
        }
        ToolStoragePayload::Summary(summary) => {
            summary.metadata.to_json().get("sanitizedResult").cloned()
        }
        _ => None,
    }
}

fn json_u32(value: &JsonValue, key: &str) -> u32 {
    value
        .get(key)
        .and_then(JsonValue::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or_default()
}

fn task_create_tool_output(response: &TaskCreateResponse, anchor: &TaskTurnItem) -> JsonValue {
    let attachment = response
        .task
        .lifecycle_policy
        .as_ref()
        .map(|policy| match policy.attachment {
            TaskAttachmentMode::Attached => "attached",
            TaskAttachmentMode::Detached => "detached",
        })
        .unwrap_or("detached");
    json!({
        "taskId": response.task.id,
        "runId": response.run.as_ref().map(|run| run.id.clone()),
        "status": task_status_label(response.task.status),
        "title": response.task.title,
        "attachment": attachment,
        "triggerKind": trigger_kind_label(response.trigger.kind()),
        "depth": anchor.depth,
        "maxDepth": anchor.max_depth,
        "childThreadId": anchor.child_thread_id,
        "childTurnId": anchor.child_turn_id,
    })
}

fn task_wait_tool_output(
    response: &pioneer_protocol::TaskWaitResponse,
    signature: &TaskWaitSignature,
) -> JsonValue {
    json!({
        "waitSignature": signature.to_json(),
        "mode": wait_mode_label(response.mode),
        "totalCount": response.total_count,
        "terminalCount": response.terminal_count,
        "pendingCount": response.pending_count,
        "completed": response.completed.iter().map(wait_item_output).collect::<Vec<_>>(),
        "failed": response.failed.iter().map(wait_item_output).collect::<Vec<_>>(),
        "cancelled": response.cancelled.iter().map(wait_item_output).collect::<Vec<_>>(),
        "pending": response.pending.iter().map(wait_item_output).collect::<Vec<_>>(),
        "timedOut": response.timed_out,
    })
}

fn task_wait_guard_output(
    signature: &TaskWaitSignature,
    previous_wait_item_id: &str,
    total_count: u32,
    terminal_count: u32,
    pending_count: u32,
    previous_timed_out: bool,
    prior_wait_count: usize,
) -> JsonValue {
    json!({
        "repeatedWait": true,
        "waitSignature": signature.to_json(),
        "previousWaitItemId": previous_wait_item_id,
        "mode": wait_mode_label(signature.mode),
        "totalCount": total_count,
        "terminalCount": terminal_count,
        "pendingCount": pending_count,
        "previousTimedOut": previous_timed_out,
        "priorWaitCount": prior_wait_count,
        "recommendation": if previous_timed_out {
            "cancel_detach_or_return_partial_result"
        } else {
            "wait_for_timeline_change_or_cancel"
        },
    })
}

fn wait_mode_label(mode: pioneer_protocol::TaskWaitMode) -> &'static str {
    match mode {
        pioneer_protocol::TaskWaitMode::AllTerminal => "all_terminal",
        pioneer_protocol::TaskWaitMode::AnyTerminal => "any_terminal",
    }
}

fn wait_item_output(item: &pioneer_protocol::TaskWaitItem) -> JsonValue {
    let run = item.run.as_ref();
    json!({
        "taskId": item.task.id,
        "runId": run.map(|run| run.id.clone()),
        "status": wait_item_status(&item.task, run),
        "summary": run.and_then(|run| run.result.as_ref()).and_then(|result| result.summary.clone()).or_else(|| item.task.result.as_ref().and_then(|result| result.summary.clone())),
        "result": run.and_then(|run| run.result.clone()).or_else(|| item.task.result.clone()),
        "error": run.and_then(|run| run.error.clone()).or_else(|| item.task.error.clone()),
        "childThreadId": item.child_thread_id,
        "childTurnId": item.child_turn_id,
    })
}

fn wait_item_status(task: &Task, run: Option<&TaskRun>) -> String {
    if let Some(run) = run {
        return run_status_label(run.status);
    }
    task_status_label(task.status)
}

fn task_summary(task: &Task) -> JsonValue {
    json!({
        "taskId": task.id,
        "title": task.title,
        "status": task_status_label(task.status),
        "ownerKind": owner_kind_label(task.owner_kind),
        "ownerId": task.owner_id,
        "parentTaskId": task.parent_task_id,
        "rootTaskId": task.root_task_id,
        "attachment": task.lifecycle_policy.as_ref().map(|policy| match policy.attachment {
            TaskAttachmentMode::Attached => "attached",
            TaskAttachmentMode::Detached => "detached",
        }),
        "createdByThreadId": task.created_by_thread_id,
        "createdByTurnId": task.created_by_turn_id,
        "createdAt": task.created_at,
        "updatedAt": task.updated_at,
    })
}

pub(crate) async fn task_turn_item_from_response(
    processor: &MessageProcessor,
    response: &TaskGetResponse,
) -> anyhow::Result<TaskTurnItem> {
    let task = &response.task;
    let run = response.runs.last();
    let trigger = response.triggers.last();
    let agent_spec = select_anchor_agent_spec(response, run);
    let lineage = match run {
        Some(run) => processor
            .crud_store
            .list_thread_lineage_for_run(run.id.as_str())
            .await?
            .into_iter()
            .last(),
        None => None,
    };
    Ok(TaskTurnItem {
        id: format!("task_item_{}", task.id),
        task_id: task.id.clone(),
        run_id: run.map(|run| run.id.clone()),
        parent_task_id: task.parent_task_id.clone(),
        root_task_id: task.root_task_id.clone(),
        title: task.title.clone(),
        status: task.status,
        trigger_kind: trigger
            .map(TaskTrigger::kind)
            .unwrap_or(TaskTriggerKind::Manual),
        executor_kind: task.executor_kind,
        child_thread_id: lineage
            .as_ref()
            .map(|lineage| lineage.child_thread_id.clone()),
        child_turn_id: lineage
            .as_ref()
            .map(|lineage| lineage.child_turn_id.clone()),
        agent_role: agent_spec.and_then(|spec| spec.agent_role.clone()),
        depth: agent_spec.map(|spec| spec.depth).unwrap_or(0),
        max_depth: agent_spec
            .map(|spec| spec.max_depth)
            .unwrap_or(DEFAULT_ROOT_MAX_DEPTH),
        next_fire_at: trigger.and_then(|trigger| trigger.next_fire_at),
        result_preview: result_preview(
            run.and_then(|run| run.result.as_ref())
                .or(task.result.as_ref()),
        ),
        error_preview: error_preview(
            run.and_then(|run| run.error.as_ref())
                .or(task.error.as_ref()),
        ),
        created_at: task.created_at,
        updated_at: task.updated_at,
    })
}

fn select_anchor_agent_spec<'a>(
    response: &'a TaskGetResponse,
    run: Option<&TaskRun>,
) -> Option<&'a TaskAgentSpec> {
    run.and_then(|run| {
        response
            .agent_specs
            .iter()
            .rev()
            .find(|spec| spec.run_id.as_deref() == Some(run.id.as_str()))
    })
    .or_else(|| response.agent_specs.iter().rev().next())
}

fn result_preview(result: Option<&TaskResult>) -> Option<String> {
    result
        .and_then(|result| result.summary.clone())
        .map(|summary| bounded_preview(summary.as_str(), 240))
}

fn error_preview(error: Option<&TaskError>) -> Option<String> {
    error.map(|error| bounded_preview(error.message.as_str(), 240))
}

fn bounded_preview(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let preview = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

fn task_status_label(status: TaskStatus) -> String {
    serde_json::to_value(status)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{status:?}").to_ascii_lowercase())
}

fn run_status_label(status: TaskRunStatus) -> String {
    serde_json::to_value(status)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{status:?}").to_ascii_lowercase())
}

fn trigger_kind_label(kind: TaskTriggerKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{kind:?}").to_ascii_lowercase())
}

fn owner_kind_label(kind: TaskOwnerKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{kind:?}").to_ascii_lowercase())
}

#[allow(dead_code)]
fn attached_immediate_lifecycle() -> TaskLifecyclePolicy {
    TaskLifecyclePolicy {
        attachment: TaskAttachmentMode::Attached,
        on_parent_cancel: TaskParentTerminalAction::Cancel,
        on_parent_failure: TaskParentTerminalAction::Cancel,
        completion: TaskCompletionBehavior::CompleteOnTerminalRun,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prior(timed_out: bool, terminal_count: u32) -> PriorWaitCall {
        PriorWaitCall {
            item_id: "wait_item".to_owned(),
            timed_out,
            terminal_count,
        }
    }

    #[test]
    fn duplicate_wait_guard_blocks_same_pending_state() {
        assert!(duplicate_wait_should_block(&[prior(false, 0)], 0, 3));
    }

    #[test]
    fn duplicate_wait_guard_allows_terminal_progress() {
        assert!(!duplicate_wait_should_block(&[prior(false, 0)], 1, 2));
    }

    #[test]
    fn duplicate_wait_guard_allows_one_timeout_retry_then_blocks() {
        assert!(!duplicate_wait_should_block(&[prior(true, 0)], 0, 3));
        assert!(duplicate_wait_should_block(
            &[prior(true, 0), prior(true, 0)],
            0,
            3
        ));
    }
}
