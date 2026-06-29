use crate::message::{MessageProcessor, now_timestamp_secs};
use async_trait::async_trait;
use pioneer_agent::{
    PendingAttachedTask, ReviewRequiredTaskObservation, TaskToolMaterialization, TaskToolProvider,
    TaskTurnContext, TerminalTaskObservation,
};
use pioneer_protocol::{
    ItemCompletedNotification, ItemUpdatedNotification, Task, TaskAcceptParams, TaskAcceptResponse,
    TaskAgentContextPolicy, TaskAgentInput, TaskAgentPrompt, TaskAgentResultContract,
    TaskAgentSpec, TaskAgentSpecInput, TaskAttachmentMode, TaskCancelParams, TaskCancelScope,
    TaskCreateParams, TaskCreateResponse, TaskDeliveryPolicy, TaskDependencyTriggerPolicy,
    TaskDetachParams, TaskError, TaskExecutorKind, TaskExternalTriggerFilter, TaskGetParams,
    TaskGetResponse, TaskLifecyclePolicy, TaskListParams, TaskManualActor, TaskMetadata,
    TaskOwnerKind, TaskPauseParams, TaskRescheduleParams, TaskResult, TaskResultCandidateStatus,
    TaskResultReviewerKind, TaskResumeParams, TaskRetryPolicy, TaskReviseParams,
    TaskReviseResponse, TaskRun, TaskRunStatus, TaskRunThreadBindingKind, TaskStatus,
    TaskTimeoutPolicy, TaskTrigger, TaskTriggerCatchUpPolicy, TaskTriggerInput, TaskTriggerKind,
    TaskTriggerSpec, TaskTurnItem, TaskUpdateParams, TaskUpdateResponse, TaskWaitMode,
    TaskWaitParams, ToolCallStatus, ToolStoragePayload, TurnItem, TurnItemEventPayload,
    constants::events,
};
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, FunctionToolOutput, PayloadKind, ToolError,
    ToolExtensionBundle, ToolHandler, ToolIdempotencyMode, ToolInvocation, ToolOutput, ToolPayload,
    ToolRecoveryMetadata, ToolRetryClass, ToolSpec, dynamic_unknown_output_policy,
    normalize_tool_arguments_from_schema,
};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::task::JoinHandle;

const TASK_CREATE_TOOL: &str = "task_create";
const TASK_WAIT_TOOL: &str = "task_wait";
const TASK_ACCEPT_TOOL: &str = "task_accept";
const TASK_REVISE_TOOL: &str = "task_revise";
const TASK_CANCEL_TOOL: &str = "task_cancel";
const TASK_UPDATE_TOOL: &str = "task_update";
const TASK_DETACH_TOOL: &str = "task_detach";
const TASK_LIST_TOOL: &str = "task_list";
const TASK_GET_TOOL: &str = "task_get";
const TASK_RESCHEDULE_TOOL: &str = "task_reschedule";
const TASK_PAUSE_TOOL: &str = "task_pause";
const TASK_RESUME_TOOL: &str = "task_resume";
const DEFAULT_ROOT_MAX_DEPTH: i64 = 3;
const DEFAULT_TASK_LIST_LIMIT: u32 = 20;
const GUARD_TASK_LIST_LIMIT: u32 = 500;

type TaskToolFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// Task tool handlers compose large protocol and CRUD futures; this is the explicit heap boundary.
fn task_tool_future<'a, F, T>(future: F) -> TaskToolFuture<'a, T>
where
    F: Future<Output = T> + Send + 'a,
{
    Box::pin(future)
}

struct AbortOnDropJoinHandle<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T> AbortOnDropJoinHandle<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        let result = self
            .handle
            .as_mut()
            .expect("join handle should be present")
            .await;
        self.handle = None;
        result
    }
}

impl<T> Drop for AbortOnDropJoinHandle<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take()
            && !handle.is_finished()
        {
            handle.abort();
        }
    }
}

// Service calls from agent tool execution can otherwise inherit a very deep poll stack.
async fn task_tool_fresh_task<F, T>(future: F) -> Result<T, tokio::task::JoinError>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    AbortOnDropJoinHandle::new(tokio::spawn(future))
        .join()
        .await
}

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
        let handler = Arc::new(TaskToolHandler {
            processor,
            context,
            mutation_cache: Arc::new(TaskToolMutationCache::default()),
        });
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
            if task.status.is_terminal() || task.status == TaskStatus::WaitingReview {
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

    async fn review_required_attached_task_observations(
        &self,
        context: TaskTurnContext,
    ) -> Result<Vec<ReviewRequiredTaskObservation>, String> {
        let processor = self.processor()?;
        let tasks = attached_tasks_for_turn(&processor, &context)
            .await
            .map_err(|error| format!("{error:#}"))?;
        let task_ids = tasks
            .iter()
            .filter(|task| !task.status.is_terminal())
            .map(|task| task.id.clone())
            .collect::<Vec<_>>();
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        let wait_state = processor
            .task_runtime
            .service()
            .get_wait_state_snapshot(TaskWaitParams {
                task_ids,
                run_ids: Vec::new(),
                timeout_ms: Some(0),
                mode: TaskWaitMode::AllTerminalOrReviewRequired,
                return_completed: false,
                return_pending: false,
            })
            .await
            .map_err(|error| format!("{error:#}"))?;
        let mut observations = wait_state
            .review_required
            .iter()
            .map(review_required_task_observation)
            .collect::<Vec<_>>();
        observations.sort_by(|left, right| {
            left.task_id
                .cmp(&right.task_id)
                .then_with(|| left.run_id.cmp(&right.run_id))
                .then_with(|| left.candidate_id.cmp(&right.candidate_id))
        });
        Ok(observations)
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
            let child_anchor = match run {
                Some(run) => processor
                    .crud_store
                    .get_task_run_child_anchor(run.id.as_str())
                    .await
                    .map_err(|error| format!("{error:#}"))?,
                None => Default::default(),
            };
            let accepted_result = run.and_then(|run| accepted_candidate_result(&response, run));
            observations.push(TerminalTaskObservation {
                task_id: response.task.id.clone(),
                run_id: run.map(|run| run.id.clone()),
                title: response.task.title.clone(),
                status: task_status_label(response.task.status),
                summary: accepted_result
                    .and_then(|result| result.summary.clone())
                    .or_else(|| {
                        run.and_then(|run| run.result.as_ref())
                            .and_then(|result| result.summary.clone())
                    })
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
                child_thread_id: child_anchor.child_thread_id,
                child_turn_id: child_anchor.child_turn_id,
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

fn accepted_candidate_result<'a>(
    response: &'a TaskGetResponse,
    run: &TaskRun,
) -> Option<&'a TaskResult> {
    response
        .result_candidates
        .iter()
        .rev()
        .find(|candidate| {
            candidate.run_id == run.id && candidate.status == TaskResultCandidateStatus::Accepted
        })
        .and_then(|candidate| candidate.result.as_ref())
}

#[derive(Clone)]
struct TaskToolHandler {
    processor: Arc<MessageProcessor>,
    context: TaskTurnContext,
    mutation_cache: Arc<TaskToolMutationCache>,
}

#[derive(Default)]
struct TaskToolMutationCache {
    outputs: Mutex<HashMap<String, JsonValue>>,
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

#[async_trait]
impl ToolHandler for TaskToolHandler {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _trace: pioneer_tools::ToolEventTrace,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let handler = self.clone();
        match task_tool_fresh_task(async move { handler.handle_in_fresh_task(invocation).await })
            .await
        {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => Err(ToolError::execution_failed(format!(
                "task tool handler failed: {error}"
            ))),
        }
    }
}

impl TaskToolHandler {
    async fn handle_in_fresh_task(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        match invocation.tool_name.as_str() {
            TASK_CREATE_TOOL => task_tool_future(self.handle_create(invocation)).await,
            TASK_WAIT_TOOL => task_tool_future(self.handle_wait(invocation)).await,
            TASK_ACCEPT_TOOL => task_tool_future(self.handle_accept(invocation)).await,
            TASK_REVISE_TOOL => task_tool_future(self.handle_revise(invocation)).await,
            TASK_CANCEL_TOOL => task_tool_future(self.handle_cancel(invocation)).await,
            TASK_UPDATE_TOOL => task_tool_future(self.handle_update(invocation)).await,
            TASK_DETACH_TOOL => task_tool_future(self.handle_detach(invocation)).await,
            TASK_LIST_TOOL => task_tool_future(self.handle_list(invocation)).await,
            TASK_GET_TOOL => task_tool_future(self.handle_get(invocation)).await,
            TASK_RESCHEDULE_TOOL => task_tool_future(self.handle_reschedule(invocation)).await,
            TASK_PAUSE_TOOL => task_tool_future(self.handle_pause(invocation)).await,
            TASK_RESUME_TOOL => task_tool_future(self.handle_resume(invocation)).await,
            other => Err(ToolError::NotFound(other.to_owned())),
        }
    }
}

impl TaskToolHandler {
    fn mutation_cache_key(&self, invocation: &ToolInvocation) -> Option<String> {
        invocation
            .idempotency_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| format!("{}:{value}", invocation.tool_name))
    }

    async fn mutation_key_guard(&self, cache_key: Option<&str>) -> Option<OwnedMutexGuard<()>> {
        let key = cache_key?;
        let lock = {
            let mut locks = self.mutation_cache.locks.lock().await;
            locks
                .entry(key.to_owned())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        Some(lock.lock_owned().await)
    }

    async fn cached_mutation_output(&self, cache_key: Option<&str>) -> Option<JsonValue> {
        let key = cache_key?;
        let mutation_outputs = self.mutation_cache.outputs.lock().await;
        mutation_outputs.get(key).cloned()
    }

    async fn cache_mutation_output(&self, cache_key: Option<&str>, output: &JsonValue) {
        if let Some(key) = cache_key {
            let mut mutation_outputs = self.mutation_cache.outputs.lock().await;
            mutation_outputs.insert(key.to_owned(), output.clone());
        }
    }

    async fn cached_or_prior_mutation_output(
        &self,
        invocation: &ToolInvocation,
        cache_key: Option<&str>,
    ) -> Result<Option<JsonValue>, ToolError> {
        let Some(cache_key) = cache_key else {
            return Ok(None);
        };
        if let Some(output) = self.cached_mutation_output(Some(cache_key)).await {
            return Ok(Some(output));
        }
        let Some(output) = self.prior_successful_mutation_output(invocation).await? else {
            return Ok(None);
        };
        self.cache_mutation_output(Some(cache_key), &output).await;
        Ok(Some(output))
    }

    async fn handle_create(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: TaskCreateToolInput = decode_tool_args(invocation.clone())?;
        let cache_key = self.mutation_cache_key(&invocation);
        let _mutation_guard = self.mutation_key_guard(cache_key.as_deref()).await;
        if let Some(output) = self
            .cached_or_prior_mutation_output(&invocation, cache_key.as_deref())
            .await?
        {
            return Ok(function_output(output));
        }
        let params = self.create_params(input).await?;
        let service = self.processor.task_runtime.service();
        let response = match task_tool_fresh_task(async move {
            service
                .create_task(pioneer_tasks::TaskCreateContext::default(), params)
                .await
        })
        .await
        {
            Ok(response) => {
                response.map_err(|error| ToolError::execution_failed(format!("{error:#}")))?
            }
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => {
                return Err(ToolError::execution_failed(format!(
                    "task_create worker failed: {error}"
                )));
            }
        };
        let anchor = self.persist_task_anchor(response.task.id.as_str()).await?;
        let output = task_create_tool_output(&response, &anchor);
        self.cache_mutation_output(cache_key.as_deref(), &output)
            .await;
        Ok(function_output(output))
    }

    async fn handle_wait(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let current_call_id = invocation.call_id.clone();
        let input: TaskWaitToolInput = decode_tool_args(invocation)?;
        let mut params = input.into_params()?;

        if !params.return_completed && !params.return_pending {
            params.return_completed = true;
            params.return_pending = true;
        }

        let signature = TaskWaitSignature::from_params(&params);

        if let Some(guard_output) = self
            .non_waitable_scheduled_guard(&signature, &params)
            .await?
        {
            return Ok(function_output(guard_output));
        }

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

    async fn non_waitable_scheduled_guard(
        &self,
        signature: &TaskWaitSignature,
        params: &pioneer_protocol::TaskWaitParams,
    ) -> Result<Option<JsonValue>, ToolError> {
        if params.task_ids.is_empty() {
            return Ok(None);
        }

        let mut snapshot_params = params.clone();
        snapshot_params.timeout_ms = Some(0);
        snapshot_params.return_completed = true;
        snapshot_params.return_pending = true;
        let snapshot = self
            .processor
            .task_runtime
            .service()
            .get_wait_state_snapshot(snapshot_params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;

        if snapshot.non_waitable.is_empty() {
            return Ok(None);
        }
        let non_waitable_ids = snapshot
            .non_waitable
            .iter()
            .map(|item| item.item.task.id.as_str())
            .collect::<BTreeSet<_>>();
        let waitable_task_ids = params
            .task_ids
            .iter()
            .filter(|task_id| !non_waitable_ids.contains(task_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let non_waitable = snapshot
            .non_waitable
            .iter()
            .map(non_waitable_guard_item_output)
            .collect::<Vec<_>>();

        Ok(Some(task_wait_non_waitable_output(
            signature,
            non_waitable,
            waitable_task_ids,
            !params.run_ids.is_empty(),
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

        if !duplicate_wait_should_block(
            &prior,
            state.terminal_count,
            state.pending_count,
            state.review_required_count,
        ) {
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
            state.review_required_count,
            last.timed_out,
            prior.len(),
        )))
    }

    async fn handle_accept(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: TaskAcceptToolInput = decode_tool_args(invocation.clone())?;
        let params = input.into_params()?;
        let cache_key = self.mutation_cache_key(&invocation);
        let _mutation_guard = self.mutation_key_guard(cache_key.as_deref()).await;
        if let Some(output) = self
            .cached_or_prior_mutation_output(&invocation, cache_key.as_deref())
            .await?
        {
            return Ok(function_output(output));
        }

        let service = self.processor.task_runtime.service();
        let context = pioneer_tasks::TaskMutationContext::parent_agent(
            self.context.thread_id.clone(),
            self.context.turn_id.clone(),
        );
        let response = match task_tool_fresh_task(async move {
            service.accept_task_result_candidate(context, params).await
        })
        .await
        {
            Ok(response) => {
                response.map_err(|error| ToolError::execution_failed(format!("{error:#}")))?
            }
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => {
                return Err(ToolError::execution_failed(format!(
                    "task_accept worker failed: {error}"
                )));
            }
        };
        let final_answer_allowed = self.final_answer_allowed_after_accept(&response).await;
        let output = task_accept_tool_output(&response, final_answer_allowed);
        self.cache_mutation_output(cache_key.as_deref(), &output)
            .await;
        Ok(function_output(output))
    }

    async fn final_answer_allowed_after_accept(&self, response: &TaskAcceptResponse) -> bool {
        if !response.task.status.is_terminal() {
            return false;
        }
        attached_tasks_for_turn(&self.processor, &self.context)
            .await
            .map(|tasks| tasks.into_iter().all(|task| task.status.is_terminal()))
            .unwrap_or(false)
    }

    async fn handle_revise(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: TaskReviseToolInput = decode_tool_args(invocation.clone())?;
        let params = input.into_params()?;
        let cache_key = self.mutation_cache_key(&invocation);
        let _mutation_guard = self.mutation_key_guard(cache_key.as_deref()).await;
        if let Some(output) = self
            .cached_or_prior_mutation_output(&invocation, cache_key.as_deref())
            .await?
        {
            return Ok(function_output(output));
        }

        let service = self.processor.task_runtime.service();
        let executor = self.processor.task_agent_executor.clone();
        let context = pioneer_tasks::TaskMutationContext::parent_agent(
            self.context.thread_id.clone(),
            self.context.turn_id.clone(),
        );
        let response = match task_tool_fresh_task(async move {
            let revised = service
                .revise_task_result_candidate(context, params)
                .await?;
            executor.dispatch_revision_turn(revised).await
        })
        .await
        {
            Ok(response) => {
                response.map_err(|error| ToolError::execution_failed(format!("{error:#}")))?
            }
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => {
                return Err(ToolError::execution_failed(format!(
                    "task_revise worker failed: {error}"
                )));
            }
        };
        let output = task_revise_tool_output(&response);
        self.cache_mutation_output(cache_key.as_deref(), &output)
            .await;
        Ok(function_output(output))
    }

    async fn handle_cancel(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: TaskCancelToolInput = decode_tool_args(invocation.clone())?;
        let params = input.into_params()?;
        let cache_key = self.mutation_cache_key(&invocation);
        let _mutation_guard = self.mutation_key_guard(cache_key.as_deref()).await;
        if let Some(output) = self
            .cached_or_prior_mutation_output(&invocation, cache_key.as_deref())
            .await?
        {
            return Ok(function_output(output));
        }
        let response = self
            .processor
            .task_runtime
            .service()
            .cancel_task(pioneer_tasks::TaskMutationContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let output = json!({ "task": task_summary(&response.task) });
        self.cache_mutation_output(cache_key.as_deref(), &output)
            .await;
        Ok(function_output(output))
    }

    async fn handle_update(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: TaskUpdateToolInput = decode_tool_args(invocation.clone())?;
        let params = self.update_params(input).await?;
        let cache_key = self.mutation_cache_key(&invocation);
        let _mutation_guard = self.mutation_key_guard(cache_key.as_deref()).await;
        if let Some(output) = self
            .cached_or_prior_mutation_output(&invocation, cache_key.as_deref())
            .await?
        {
            return Ok(function_output(output));
        }
        let response = self
            .processor
            .task_runtime
            .service()
            .update_task(pioneer_tasks::TaskMutationContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let output = task_update_tool_output(&response);
        self.cache_mutation_output(cache_key.as_deref(), &output)
            .await;
        Ok(function_output(output))
    }

    async fn handle_detach(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: TaskIdToolInput = decode_tool_args(invocation)?;
        let params = input.into_detach_params()?;
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
        let owner_kind = input.owner_kind;
        let owner_id = normalize_task_list_owner_id(
            owner_kind,
            input.owner_id,
            self.context.thread_id.as_str(),
        )?;
        let params = TaskListParams {
            workspace_id: self.context.workspace_id.clone(),
            owner_kind,
            owner_id,
            parent_task_id: validate_optional_entity_id(input.parent_task_id, "parentTaskId")?,
            root_task_id: validate_optional_entity_id(input.root_task_id, "rootTaskId")?,
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
        let input: TaskIdToolInput = decode_tool_args(invocation)?;
        let params = input.into_get_params()?;
        let response = self
            .processor
            .task_runtime
            .service()
            .get_task(params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let lineages = task_get_legacy_lineage_output(&response);
        let payload = json!({
            "task": response.task,
            "triggers": task_trigger_details_output(&response.triggers),
            "runs": response.runs,
            "agentSpecs": response.agent_specs,
            "dependencies": response.dependencies,
            "lineage": lineages,
            "threadLineage": response.thread_lineage,
            "taskRunThreadBindings": response.task_run_thread_bindings,
            "taskRunTurns": response.task_run_turns,
            "resultCandidates": response.result_candidates,
            "resultReviewEvents": response.result_review_events,
        });
        Ok(function_output(payload))
    }

    async fn handle_reschedule(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: TaskRescheduleToolInput = decode_tool_args(invocation.clone())?;
        let params = input.into_params()?;
        let cache_key = self.mutation_cache_key(&invocation);
        let _mutation_guard = self.mutation_key_guard(cache_key.as_deref()).await;
        if let Some(output) = self
            .cached_or_prior_mutation_output(&invocation, cache_key.as_deref())
            .await?
        {
            return Ok(function_output(output));
        }
        let response = self
            .processor
            .task_runtime
            .service()
            .reschedule_task(pioneer_tasks::TaskMutationContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let output = json!({
            "task": task_summary(&response.task),
            "trigger": task_trigger_model_output(&response.trigger),
        });
        self.cache_mutation_output(cache_key.as_deref(), &output)
            .await;
        Ok(function_output(output))
    }

    async fn handle_pause(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: TaskPauseToolInput = decode_tool_args(invocation.clone())?;
        let params = input.into_params()?;
        let cache_key = self.mutation_cache_key(&invocation);
        let _mutation_guard = self.mutation_key_guard(cache_key.as_deref()).await;
        if let Some(output) = self
            .cached_or_prior_mutation_output(&invocation, cache_key.as_deref())
            .await?
        {
            return Ok(function_output(output));
        }
        let response = self
            .processor
            .task_runtime
            .service()
            .pause_task(pioneer_tasks::TaskMutationContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let output = json!({
            "task": task_summary(&response.task),
            "triggers": task_trigger_details_output(&response.triggers),
        });
        self.cache_mutation_output(cache_key.as_deref(), &output)
            .await;
        Ok(function_output(output))
    }

    async fn handle_resume(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        let input: TaskResumeToolInput = decode_tool_args(invocation.clone())?;
        let params = input.into_params()?;
        let cache_key = self.mutation_cache_key(&invocation);
        let _mutation_guard = self.mutation_key_guard(cache_key.as_deref()).await;
        if let Some(output) = self
            .cached_or_prior_mutation_output(&invocation, cache_key.as_deref())
            .await?
        {
            return Ok(function_output(output));
        }
        let response = self
            .processor
            .task_runtime
            .service()
            .resume_task(pioneer_tasks::TaskMutationContext::default(), params)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let output = json!({
            "task": task_summary(&response.task),
            "triggers": task_trigger_details_output(&response.triggers),
        });
        self.cache_mutation_output(cache_key.as_deref(), &output)
            .await;
        Ok(function_output(output))
    }

    async fn create_params(
        &self,
        input: TaskCreateToolInput,
    ) -> Result<TaskCreateParams, ToolError> {
        let title = required_tool_string(Some(input.title.as_str()), "title")?;
        let goal = required_tool_string(Some(input.goal.as_str()), "goal")?;
        let (model, model_provider) =
            current_thread_model_identity(&self.processor, &self.context).await?;
        let trigger = input
            .trigger
            .unwrap_or(TaskTriggerToolInput::Immediate)
            .into_trigger_input()?;
        let executor_kind = input
            .executor_kind
            .unwrap_or(TaskToolExecutorKind::Agent)
            .into_executor_kind();
        let parent_task_id = current_parent_task_id(&self.processor, &self.context)
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        let inherited_max_depth =
            inherited_max_depth(&self.processor, parent_task_id.as_deref()).await?;
        let requested_max_depth = input.max_depth.unwrap_or(inherited_max_depth);
        if requested_max_depth < 0 {
            return Err(ToolError::invalid_arguments(
                "`maxDepth` must be greater than or equal to 0",
            ));
        }
        let trigger_kind = trigger.spec.kind();
        let instructions = normalize_task_instructions(input.instructions, trigger_kind)?;
        let output_instructions =
            normalize_task_output_instructions(input.output_instructions, trigger_kind)?;
        let prompt_input = merge_task_agent_input(input.input_text, input.input)?;
        let prompt = TaskAgentPrompt {
            goal: goal.clone(),
            instructions,
            input: prompt_input,
            output_instructions,
        };
        let permission_cap = current_turn_permission_cap(&self.processor, &self.context).await?;
        let agent_spec = (executor_kind == TaskExecutorKind::Agent).then_some(TaskAgentSpecInput {
            agent_role: input.agent_role,
            agent_nickname: input.agent_nickname,
            model: Some(model),
            model_provider: Some(model_provider),
            prompt,
            context_policy: input.context_policy,
            tool_policy: input.tool_policy,
            permission_cap: Some(permission_cap),
            result_contract: input.result_contract,
            review_policy: None,
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

    async fn prior_successful_mutation_output(
        &self,
        invocation: &ToolInvocation,
    ) -> Result<Option<JsonValue>, ToolError> {
        let Some(current_key) = invocation
            .idempotency_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return Ok(None);
        };
        let Some(parent_events) = self
            .processor
            .crud_store
            .get_turn_item_events(
                self.context.thread_id.as_str(),
                self.context.turn_id.as_str(),
            )
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?
        else {
            return Ok(None);
        };

        for event in parent_events.events.into_iter().rev() {
            let TurnItemEventPayload::ItemCompleted { item, .. } = event.payload else {
                continue;
            };
            let TurnItem::DynamicToolCall {
                id,
                tool_name,
                arguments,
                status,
                storage,
                success,
                ..
            } = item
            else {
                continue;
            };
            if tool_name != invocation.tool_name
                || status != ToolCallStatus::Completed
                || success == Some(false)
            {
                continue;
            }
            if mutation_idempotency_key_for_item(tool_name.as_str(), id.as_str(), &arguments)
                .as_deref()
                != Some(current_key)
            {
                continue;
            }
            if let Some(output) = wait_result_from_storage(&storage) {
                return Ok(Some(output));
            }
        }

        Ok(None)
    }

    async fn update_params(
        &self,
        input: TaskUpdateToolInput,
    ) -> Result<TaskUpdateParams, ToolError> {
        if !input.has_patch_fields() {
            return Err(ToolError::invalid_arguments(
                "`task_update` requires at least one field to change",
            ));
        }
        if input.clear_input && (input.input_text.is_some() || input.input.is_some()) {
            return Err(ToolError::invalid_arguments(
                "`task_update` cannot clear and set input in the same request",
            ));
        }

        let task_id = validate_entity_id(input.task_id, "taskId")?;
        let trigger = input
            .trigger
            .map(TaskTriggerToolInput::into_trigger_input)
            .transpose()?;
        let trigger_kind = match trigger.as_ref() {
            Some(trigger) => trigger.spec.kind(),
            None => self.current_task_trigger_kind(task_id.as_str()).await?,
        };
        let instructions = input
            .instructions
            .map(|instructions| normalize_task_instructions(instructions, trigger_kind))
            .transpose()?;
        let output_instructions = input
            .output_instructions
            .map(Some)
            .map(|output| normalize_task_output_instructions(output, trigger_kind))
            .transpose()?
            .flatten();
        let merged_input = merge_task_agent_input(input.input_text, input.input)?;

        Ok(TaskUpdateParams {
            task_id,
            expected_revision: input.expected_revision,
            title: input.title,
            goal: input.goal,
            priority: input.priority,
            trigger,
            agent_role: input.agent_role,
            agent_nickname: input.agent_nickname,
            instructions,
            input_text: None,
            input: merged_input,
            output_instructions,
            context_policy: input.context_policy,
            tool_policy: input.tool_policy,
            result_contract: input.result_contract,
            lifecycle_policy: input.lifecycle_policy,
            delivery_policy: input.delivery_policy,
            retry_policy: input.retry_policy,
            timeout_policy: input.timeout_policy,
            concurrency_policy: input.concurrency_policy,
            metadata: input.metadata,
            clear_agent_role: input.clear_agent_role,
            clear_agent_nickname: input.clear_agent_nickname,
            clear_input: input.clear_input,
            clear_output_instructions: input.clear_output_instructions,
            clear_context_policy: input.clear_context_policy,
            clear_tool_policy: input.clear_tool_policy,
            clear_result_contract: input.clear_result_contract,
            clear_timeout_policy: input.clear_timeout_policy,
            clear_concurrency_policy: input.clear_concurrency_policy,
            clear_metadata: input.clear_metadata,
        })
    }

    async fn current_task_trigger_kind(&self, task_id: &str) -> Result<TaskTriggerKind, ToolError> {
        let response = self
            .processor
            .task_runtime
            .service()
            .get_task(TaskGetParams {
                task_id: task_id.to_owned(),
            })
            .await
            .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;
        Ok(response
            .triggers
            .last()
            .map(TaskTrigger::kind)
            .unwrap_or(TaskTriggerKind::Immediate))
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

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Model-facing input for task_create. Do not pass runtime-owned fields such as workspaceId, ownerKind, parentTaskId, rootTaskId, depth, model, modelProvider, or trigger.spec.
struct TaskCreateToolInput {
    /// Short human-readable task title. For child agents this becomes the hidden child thread title.
    title: String,
    /// Short concrete objective for the task executor. Put durable run instructions in instructions, task data in inputText/input, and result format in outputInstructions.
    goal: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Omit for an immediate attached subagent. Use this object directly; do not wrap it in spec, schedule, or triggerInput. For daily scheduled work choose the cron trigger kind and fill its leaf fields.
    trigger: Option<TaskTriggerToolInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Model-facing task tools currently create agent tasks only. Omit this field unless explicitly setting "agent".
    executor_kind: Option<TaskToolExecutorKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional role label for the subagent, for example "researcher", "reviewer", or "implementer".
    agent_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional short display name for the subagent.
    agent_nickname: Option<String>,
    #[serde(default)]
    /// Step-by-step executor instructions. For scheduled, interval, and cron tasks this is required and must be self-contained for future runs: say what to do, how to choose currently available tools/skills/MCP/built-ins by capability, when to fail clearly, and not to rely on hidden chat context or tool names from task creation time. Immediate subagents may omit this for a concise default.
    instructions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Plain text task input and parameters, not behavior instructions. Prefer inputText over input for simple subagent tasks, for example city, path, date range, language, or user-provided facts.
    input_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Structured task input with variables, attachments, and references. Use this for data the future run should consume; keep execution behavior in instructions.
    input: Option<TaskAgentInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Final result format and delivery contract. Required for scheduled, interval, and cron tasks; specify language, markdown/json structure, required fields, and how to report failure.
    output_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Advanced context policy for the child agent. Omit unless the user asks for custom context handling.
    context_policy: Option<TaskAgentContextPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Advanced tool policy for the child agent. Omit unless restricting tools or write access is required.
    tool_policy: Option<pioneer_protocol::TaskAgentToolPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional result contract. Omit for normal text or markdown subagent results.
    result_contract: Option<TaskAgentResultContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    /// Maximum allowed subagent nesting depth from this task. Omit to inherit the runtime default.
    max_depth: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional scheduling priority. Higher values are preferred by the task service.
    priority: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Advanced lifecycle policy. Omit for attached subagent work.
    lifecycle_policy: Option<TaskLifecyclePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Advanced delivery policy for scheduled or detached work. Omit for attached subagent work.
    delivery_policy: Option<TaskDeliveryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Advanced retry policy. Omit to use the task service default.
    retry_policy: Option<TaskRetryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Advanced timeout policy. Omit to use the task service default.
    timeout_policy: Option<TaskTimeoutPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Advanced concurrency policy. Omit unless the task must serialize access to a shared resource.
    concurrency_policy: Option<pioneer_protocol::TaskConcurrencyPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional labels or structured metadata for later task lookup.
    metadata: Option<TaskMetadata>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "idempotency_key"
    )]
    /// Optional mutation idempotency key. Omit in normal model calls; runtime derives one from the tool call id.
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// Model-facing executor kind. Only "agent" is currently supported by task_create.
enum TaskToolExecutorKind {
    Agent,
}

impl TaskToolExecutorKind {
    fn into_executor_kind(self) -> TaskExecutorKind {
        match self {
            Self::Agent => TaskExecutorKind::Agent,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
/// Model-facing trigger union. Use it directly as trigger; do not use the internal spec wrapper and do not put trigger-specific fields at the top level.
enum TaskTriggerToolInput {
    /// Run immediately. This is the default when trigger is omitted.
    Immediate,
    /// Run once at a Unix timestamp in seconds.
    ScheduledAt {
        #[serde(rename = "scheduledAt")]
        #[schemars(range(min = 1))]
        /// Unix timestamp in seconds. Do not pass natural language dates here. Example value: 1893456000.
        scheduled_at: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Optional IANA timezone label for display and scheduling context. Example value: Europe/Moscow.
        timezone: Option<String>,
        #[serde(
            default,
            rename = "catchUpPolicy",
            skip_serializing_if = "Option::is_none"
        )]
        /// Optional missed-fire policy. Defaults to run_once_for_latest_missed.
        catch_up_policy: Option<TaskTriggerCatchUpPolicy>,
    },
    /// Run every N seconds. The optional anchor is a Unix timestamp in seconds.
    Interval {
        #[serde(rename = "intervalSeconds")]
        #[schemars(range(min = 1))]
        /// Positive repeat interval in seconds. Example value: 900.
        interval_seconds: i64,
        #[serde(
            default,
            rename = "intervalAnchorAt",
            skip_serializing_if = "Option::is_none"
        )]
        #[schemars(range(min = 1))]
        /// Optional Unix timestamp in seconds used as the recurring schedule anchor. Example value: 1893456000.
        interval_anchor_at: Option<i64>,
        #[serde(
            default,
            rename = "catchUpPolicy",
            skip_serializing_if = "Option::is_none"
        )]
        /// Optional missed-fire policy. Defaults to run_once_for_latest_missed.
        catch_up_policy: Option<TaskTriggerCatchUpPolicy>,
    },
    /// Run on a five-field cron expression in a concrete IANA timezone.
    Cron {
        #[serde(rename = "cronExpr")]
        /// Five-field cron expression: minute hour day-of-month month day-of-week. Example value: 0 7 * * *.
        cron_expr: String,
        /// Required IANA timezone. Example value: Europe/Moscow.
        timezone: String,
        #[serde(
            default,
            rename = "catchUpPolicy",
            skip_serializing_if = "Option::is_none"
        )]
        /// Optional missed-fire policy. Defaults to run_once_for_latest_missed.
        catch_up_policy: Option<TaskTriggerCatchUpPolicy>,
    },
    /// Create a dormant task that must be triggered manually by an allowed actor.
    Manual {
        #[serde(
            default,
            rename = "allowedActor",
            skip_serializing_if = "Option::is_none"
        )]
        /// Optional actor allowed to manually fire the task.
        allowed_actor: Option<TaskManualActor>,
    },
    /// Trigger from an external event source.
    External {
        /// External source name. Example value: calendar.webhook.
        source: String,
        #[serde(default, rename = "eventType", skip_serializing_if = "Option::is_none")]
        /// Optional external event type. Example value: event.created.
        event_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        /// Optional structured event filter.
        filter: Option<TaskExternalTriggerFilter>,
    },
    /// Trigger when dependency policy is satisfied.
    Dependency { policy: TaskDependencyTriggerPolicy },
}

impl TaskTriggerToolInput {
    fn into_trigger_input(self) -> Result<TaskTriggerInput, ToolError> {
        let spec = match self {
            Self::Immediate => TaskTriggerSpec::Immediate,
            Self::ScheduledAt {
                scheduled_at,
                timezone,
                catch_up_policy,
            } => {
                if scheduled_at <= 0 {
                    return Err(ToolError::invalid_arguments(
                        "`trigger.scheduledAt` must be a positive Unix timestamp in seconds",
                    ));
                }
                TaskTriggerSpec::ScheduledAt {
                    scheduled_at,
                    timezone: clean_optional_string(timezone),
                    catch_up_policy,
                }
            }
            Self::Interval {
                interval_seconds,
                interval_anchor_at,
                catch_up_policy,
            } => {
                if interval_seconds <= 0 {
                    return Err(ToolError::invalid_arguments(
                        "`trigger.intervalSeconds` must be greater than 0",
                    ));
                }
                TaskTriggerSpec::Interval {
                    interval_seconds,
                    interval_anchor_at,
                    catch_up_policy,
                }
            }
            Self::Cron {
                cron_expr,
                timezone,
                catch_up_policy,
            } => TaskTriggerSpec::Cron {
                cron_expr: required_tool_string(Some(cron_expr.as_str()), "trigger.cronExpr")?,
                timezone: required_tool_string(Some(timezone.as_str()), "trigger.timezone")?,
                catch_up_policy,
            },
            Self::Manual { allowed_actor } => TaskTriggerSpec::Manual { allowed_actor },
            Self::External {
                source,
                event_type,
                filter,
            } => TaskTriggerSpec::External {
                source: required_tool_string(Some(source.as_str()), "trigger.source")?,
                event_type: clean_optional_string(event_type),
                filter,
            },
            Self::Dependency { policy } => TaskTriggerSpec::Dependency { policy },
        };
        Ok(TaskTriggerInput { spec })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Model-facing input for task_wait. Provide taskIds and/or runIds arrays; do not use a single taskId field.
struct TaskWaitToolInput {
    /// Active attached task ids to join. Each Pioneer entity id is exactly 21 characters. Use taskIds, not taskId, even for one task. Do not wait on scheduled/interval/cron tasks with waitable=false or runId=null; confirm their schedule instead.
    #[serde(default)]
    #[schemars(inner(length(min = 21, max = 21)))]
    task_ids: Vec<String>,
    /// Task run ids to join. Each Pioneer entity id is exactly 21 characters.
    #[serde(default)]
    #[schemars(inner(length(min = 21, max = 21)))]
    run_ids: Vec<String>,
    /// Optional timeout in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    timeout_ms: Option<u64>,
    /// all_terminal waits for every target to become terminal. By default this tool also returns when every target is terminal or ready for review.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mode: Option<TaskWaitMode>,
    /// Include terminal task results in the response. The handler enables this by default when both return flags are false.
    #[serde(default)]
    return_completed: bool,
    /// Include still-pending task state in timeout responses. The handler enables this by default when both return flags are false.
    #[serde(default)]
    return_pending: bool,
}

impl TaskWaitToolInput {
    fn into_params(self) -> Result<pioneer_protocol::TaskWaitParams, ToolError> {
        let task_ids = validate_id_list(self.task_ids, "taskIds")?;
        let run_ids = validate_id_list(self.run_ids, "runIds")?;
        if task_ids.is_empty() && run_ids.is_empty() {
            return Err(ToolError::invalid_arguments(
                "`task_wait` requires at least one id in `taskIds` or `runIds`",
            ));
        }
        Ok(pioneer_protocol::TaskWaitParams {
            task_ids,
            run_ids,
            timeout_ms: self.timeout_ms,
            mode: self
                .mode
                .unwrap_or(TaskWaitMode::AllTerminalOrReviewRequired),
            return_completed: self.return_completed,
            return_pending: self.return_pending,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Model-facing input for task_accept. Use this only for a candidate returned by task_wait reviewRequired.
struct TaskAcceptToolInput {
    /// Task id that owns the candidate. Pioneer entity ids are exactly 21 characters.
    #[schemars(length(min = 21, max = 21))]
    task_id: String,
    /// Task run id that produced the candidate. Pioneer entity ids are exactly 21 characters.
    #[schemars(length(min = 21, max = 21))]
    run_id: String,
    /// Candidate id to accept. Use the exact candidateId returned by task_wait reviewRequired.
    #[schemars(length(min = 1, max = 128))]
    candidate_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional short reason for the acceptance.
    reason: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "idempotency_key"
    )]
    /// Optional mutation idempotency key. Omit in normal model calls; runtime derives one from the tool call id.
    idempotency_key: Option<String>,
}

impl TaskAcceptToolInput {
    fn into_params(self) -> Result<TaskAcceptParams, ToolError> {
        Ok(TaskAcceptParams {
            task_id: validate_entity_id(self.task_id, "taskId")?,
            run_id: validate_entity_id(self.run_id, "runId")?,
            candidate_id: validate_candidate_id(self.candidate_id, "candidateId")?,
            reason: clean_optional_string(self.reason),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Model-facing input for task_revise. Use this only for a candidate returned by task_wait reviewRequired.
struct TaskReviseToolInput {
    /// Task id that owns the candidate. Pioneer entity ids are exactly 21 characters.
    #[schemars(length(min = 21, max = 21))]
    task_id: String,
    /// Task run id that produced the candidate. Pioneer entity ids are exactly 21 characters.
    #[schemars(length(min = 21, max = 21))]
    run_id: String,
    /// Candidate id to reject and revise. Use the exact candidateId returned by task_wait reviewRequired.
    #[schemars(length(min = 1, max = 128))]
    candidate_id: String,
    /// Concrete feedback explaining what is wrong and what the child must fix.
    #[schemars(length(min = 1, max = 16000))]
    feedback: String,
    #[serde(default)]
    /// Optional additional instructions for the revision turn.
    additional_instructions: Vec<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "idempotency_key"
    )]
    /// Optional mutation idempotency key. Omit in normal model calls; runtime derives one from the tool call id.
    idempotency_key: Option<String>,
}

impl TaskReviseToolInput {
    fn into_params(self) -> Result<TaskReviseParams, ToolError> {
        let feedback = self.feedback.trim().to_owned();
        if feedback.is_empty() {
            return Err(ToolError::invalid_arguments("feedback must be non-empty"));
        }
        Ok(TaskReviseParams {
            task_id: validate_entity_id(self.task_id, "taskId")?,
            run_id: validate_entity_id(self.run_id, "runId")?,
            candidate_id: validate_candidate_id(self.candidate_id, "candidateId")?,
            feedback,
            additional_instructions: self
                .additional_instructions
                .into_iter()
                .filter_map(|value| clean_optional_string(Some(value)))
                .collect(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Model-facing input for task_cancel.
struct TaskCancelToolInput {
    /// Task id to cancel. Pioneer entity ids are exactly 21 characters.
    #[schemars(length(min = 21, max = 21))]
    task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional human-readable cancellation reason.
    reason: Option<String>,
    #[serde(default)]
    /// Cancellation scope. Defaults to attached_subtree.
    scope: TaskCancelScope,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "idempotency_key"
    )]
    /// Optional mutation idempotency key. Omit in normal model calls; runtime derives one from the tool call id.
    idempotency_key: Option<String>,
}

impl TaskCancelToolInput {
    fn into_params(self) -> Result<TaskCancelParams, ToolError> {
        Ok(TaskCancelParams {
            task_id: validate_entity_id(self.task_id, "taskId")?,
            reason: clean_optional_string(self.reason),
            scope: self.scope,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Model-facing input for task_update. Patch only the fields that should change; omitted fields keep their current value. Use clear* flags to explicitly remove optional values.
struct TaskUpdateToolInput {
    /// Task id to update. Pioneer entity ids are exactly 21 characters.
    #[schemars(length(min = 21, max = 21))]
    task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional optimistic-concurrency guard. If supplied, the task service rejects the update when the current revision differs.
    expected_revision: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement task title. For child agents this is also the human-facing task label.
    title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement short objective. Keep durable execution steps in instructions.
    goal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement scheduling priority.
    priority: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement trigger. Use this object directly; do not wrap it in spec. For daily scheduled work choose the cron trigger kind and fill its leaf fields.
    trigger: Option<TaskTriggerToolInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement role label for an agent task.
    agent_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement display name for an agent task.
    agent_nickname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement self-contained executor instructions. For scheduled, interval, and cron tasks these must describe future-run behavior, runtime capability selection, and failure conditions.
    instructions: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement plain text task input and parameters. Prefer inputText over input for simple updates.
    input_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement structured task input with variables, attachments, and references.
    input: Option<TaskAgentInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement final result format and delivery contract. Required by the service for scheduled, interval, and cron agent tasks.
    output_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement advanced context policy.
    context_policy: Option<TaskAgentContextPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement advanced tool policy.
    tool_policy: Option<pioneer_protocol::TaskAgentToolPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement result contract.
    result_contract: Option<TaskAgentResultContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement lifecycle policy.
    lifecycle_policy: Option<TaskLifecyclePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement delivery policy.
    delivery_policy: Option<TaskDeliveryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement retry policy.
    retry_policy: Option<TaskRetryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement timeout policy.
    timeout_policy: Option<TaskTimeoutPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement concurrency policy.
    concurrency_policy: Option<pioneer_protocol::TaskConcurrencyPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Replacement labels or structured metadata.
    metadata: Option<TaskMetadata>,
    #[serde(default)]
    /// Clear the agent role.
    clear_agent_role: bool,
    #[serde(default)]
    /// Clear the agent nickname.
    clear_agent_nickname: bool,
    #[serde(default)]
    /// Clear agent input.
    clear_input: bool,
    #[serde(default)]
    /// Clear output instructions. Scheduled, interval, and cron agent tasks cannot remain without output instructions.
    clear_output_instructions: bool,
    #[serde(default)]
    /// Clear context policy.
    clear_context_policy: bool,
    #[serde(default)]
    /// Clear tool policy.
    clear_tool_policy: bool,
    #[serde(default)]
    /// Clear result contract.
    clear_result_contract: bool,
    #[serde(default)]
    /// Clear timeout policy.
    clear_timeout_policy: bool,
    #[serde(default)]
    /// Clear concurrency policy.
    clear_concurrency_policy: bool,
    #[serde(default)]
    /// Clear metadata.
    clear_metadata: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "idempotency_key"
    )]
    /// Optional mutation idempotency key. Omit in normal model calls; runtime derives one from the tool call id.
    idempotency_key: Option<String>,
}

impl TaskUpdateToolInput {
    fn has_patch_fields(&self) -> bool {
        self.title.is_some()
            || self.goal.is_some()
            || self.priority.is_some()
            || self.trigger.is_some()
            || self.agent_role.is_some()
            || self.agent_nickname.is_some()
            || self.instructions.is_some()
            || self.input_text.is_some()
            || self.input.is_some()
            || self.output_instructions.is_some()
            || self.context_policy.is_some()
            || self.tool_policy.is_some()
            || self.result_contract.is_some()
            || self.lifecycle_policy.is_some()
            || self.delivery_policy.is_some()
            || self.retry_policy.is_some()
            || self.timeout_policy.is_some()
            || self.concurrency_policy.is_some()
            || self.metadata.is_some()
            || self.clear_agent_role
            || self.clear_agent_nickname
            || self.clear_input
            || self.clear_output_instructions
            || self.clear_context_policy
            || self.clear_tool_policy
            || self.clear_result_contract
            || self.clear_timeout_policy
            || self.clear_concurrency_policy
            || self.clear_metadata
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Model-facing input for task_get and task_detach.
struct TaskIdToolInput {
    /// Task id. Pioneer entity ids are exactly 21 characters.
    #[schemars(length(min = 21, max = 21))]
    task_id: String,
}

impl TaskIdToolInput {
    fn into_get_params(self) -> Result<TaskGetParams, ToolError> {
        Ok(TaskGetParams {
            task_id: validate_entity_id(self.task_id, "taskId")?,
        })
    }

    fn into_detach_params(self) -> Result<TaskDetachParams, ToolError> {
        Ok(TaskDetachParams {
            task_id: validate_entity_id(self.task_id, "taskId")?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Model-facing filters for task_list. Workspace is derived from the current thread and must not be supplied.
struct TaskListToolInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional owner kind filter. With ownerKind=thread and omitted ownerId, task_list uses the current thread.
    owner_kind: Option<TaskOwnerKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional owner id filter. Omit it with ownerKind=thread to list tasks owned by the current thread.
    owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 21, max = 21))]
    /// Optional parent task id filter. Pioneer entity ids are exactly 21 characters.
    parent_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 21, max = 21))]
    /// Optional root task id filter. Pioneer entity ids are exactly 21 characters.
    root_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional task status filter.
    status: Option<TaskStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = 100))]
    /// Maximum number of tasks to return. The handler clamps this to 1..=100.
    limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Model-facing input for task_reschedule.
struct TaskRescheduleToolInput {
    /// Task id to reschedule. Pioneer entity ids are exactly 21 characters.
    #[schemars(length(min = 21, max = 21))]
    task_id: String,
    /// New model-facing trigger. Use this object directly; do not wrap it in spec.
    trigger: TaskTriggerToolInput,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "idempotency_key"
    )]
    /// Optional mutation idempotency key. Omit in normal model calls; runtime derives one from the tool call id.
    idempotency_key: Option<String>,
}

impl TaskRescheduleToolInput {
    fn into_params(self) -> Result<TaskRescheduleParams, ToolError> {
        Ok(TaskRescheduleParams {
            task_id: validate_entity_id(self.task_id, "taskId")?,
            trigger: self.trigger.into_trigger_input()?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Model-facing input for task_pause.
struct TaskPauseToolInput {
    /// Task id to pause. Pioneer entity ids are exactly 21 characters.
    #[schemars(length(min = 21, max = 21))]
    task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional human-readable pause reason.
    reason: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "idempotency_key"
    )]
    /// Optional mutation idempotency key. Omit in normal model calls; runtime derives one from the tool call id.
    idempotency_key: Option<String>,
}

impl TaskPauseToolInput {
    fn into_params(self) -> Result<TaskPauseParams, ToolError> {
        Ok(TaskPauseParams {
            task_id: validate_entity_id(self.task_id, "taskId")?,
            reason: clean_optional_string(self.reason),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
/// Model-facing input for task_resume.
struct TaskResumeToolInput {
    /// Task id to resume. Pioneer entity ids are exactly 21 characters.
    #[schemars(length(min = 21, max = 21))]
    task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional human-readable resume reason.
    reason: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        alias = "idempotency_key"
    )]
    /// Optional mutation idempotency key. Omit in normal model calls; runtime derives one from the tool call id.
    idempotency_key: Option<String>,
}

impl TaskResumeToolInput {
    fn into_params(self) -> Result<TaskResumeParams, ToolError> {
        Ok(TaskResumeParams {
            task_id: validate_entity_id(self.task_id, "taskId")?,
            reason: clean_optional_string(self.reason),
        })
    }
}

fn task_tool_specs() -> Vec<ConfiguredToolSpec> {
    vec![
        task_tool_spec(
            TASK_CREATE_TOOL,
            "Create a durable task or subagent. Use existing fields: goal is the short objective, instructions is the self-contained future-run prompt, inputText/input is task data, and outputInstructions is the final result format. For scheduled, interval, and cron work, instructions and outputInstructions are required; instructions must tell the future agent to use currently available tools/skills/MCP/built-ins by capability and fail clearly if required capability or data is unavailable. For immediate subagents omit trigger. For scheduled work use trigger directly, choose the trigger kind, and fill trigger leaf fields such as cronExpr and timezone. Do not wrap trigger in spec. Parent/root/depth context is derived by runtime and must not be supplied.",
            task_create_schema(),
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Arguments,
                idempotency_mode: ToolIdempotencyMode::RequiresKey,
                max_attempts: 1,
                can_resume: false,
                max_wall_clock_secs: None,
            },
        ),
        task_tool_spec(
            TASK_WAIT_TOOL,
            "Join one or more active attached task ids or run ids using the task event bus. Use once for immediate attached tasks that have a runId or task_create returned waitable=true; by default it returns when all targets are terminal or ready for review, or on timeout. Do not call task_wait after creating scheduled, interval, or cron tasks when task_create returned waitable=false/runId=null; confirm the schedule instead. Do not repeatedly call it for the same task set unless the prior wait timed out.",
            task_wait_schema(),
            ToolRecoveryMetadata {
                retry_class: ToolRetryClass::Transient,
                idempotency_mode: ToolIdempotencyMode::Safe,
                max_attempts: 2,
                can_resume: true,
                max_wall_clock_secs: Some(3_600),
            },
        ),
        task_tool_spec(
            TASK_ACCEPT_TOOL,
            "Accept a pending review candidate returned by task_wait. This records the parent-agent approval, makes the candidate final, and completes the child task/run with that result.",
            task_accept_schema(),
            safe_mutation_recovery(),
        ),
        task_tool_spec(
            TASK_REVISE_TOOL,
            "Reject a review candidate returned by task_wait and send concrete feedback to the same child thread for another revision turn.",
            task_revise_schema(),
            safe_mutation_recovery(),
        ),
        task_tool_spec(
            TASK_CANCEL_TOOL,
            "Cancel a task through the task service.",
            task_cancel_schema(),
            safe_mutation_recovery(),
        ),
        task_tool_spec(
            TASK_UPDATE_TOOL,
            "Update a non-terminal task through the task service. Patch only fields that should change; use existing prompt fields instructions, inputText/input, and outputInstructions. inputText and input may be supplied together: inputText fills input.text when input.text is absent; different values are rejected. For scheduled, interval, and cron agent tasks, instructions and outputInstructions must remain self-contained and valid for future runs. Pass trigger directly, choose the trigger kind, and fill trigger leaf fields such as cronExpr and timezone. Do not wrap trigger in spec.",
            task_update_schema(),
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
            "Reschedule a non-terminal task through the task service. Pass trigger directly, choose the trigger kind, and fill trigger leaf fields such as scheduledAt, cronExpr, and timezone. Do not wrap trigger in spec.",
            task_reschedule_schema(),
            safe_mutation_recovery(),
        ),
        task_tool_spec(
            TASK_PAUSE_TOOL,
            "Pause a non-terminal task through the task service. Running task runs are not cancelled.",
            task_pause_schema(),
            safe_mutation_recovery(),
        ),
        task_tool_spec(
            TASK_RESUME_TOOL,
            "Resume a paused task through the task service and recompute its next scheduled fire.",
            task_resume_schema(),
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
        max_wall_clock_secs: None,
    }
}

fn safe_mutation_recovery() -> ToolRecoveryMetadata {
    ToolRecoveryMetadata {
        retry_class: ToolRetryClass::Transient,
        idempotency_mode: ToolIdempotencyMode::RequiresKey,
        max_attempts: 1,
        can_resume: false,
        max_wall_clock_secs: None,
    }
}

fn task_create_schema() -> JsonValue {
    tool_input_schema::<TaskCreateToolInput>()
}

fn task_wait_schema() -> JsonValue {
    tool_input_schema::<TaskWaitToolInput>()
}

fn task_accept_schema() -> JsonValue {
    tool_input_schema::<TaskAcceptToolInput>()
}

fn task_revise_schema() -> JsonValue {
    tool_input_schema::<TaskReviseToolInput>()
}

fn task_cancel_schema() -> JsonValue {
    tool_input_schema::<TaskCancelToolInput>()
}

fn task_update_schema() -> JsonValue {
    tool_input_schema::<TaskUpdateToolInput>()
}

fn task_id_schema() -> JsonValue {
    tool_input_schema::<TaskIdToolInput>()
}

fn task_list_schema() -> JsonValue {
    tool_input_schema::<TaskListToolInput>()
}

fn task_reschedule_schema() -> JsonValue {
    tool_input_schema::<TaskRescheduleToolInput>()
}

fn task_pause_schema() -> JsonValue {
    tool_input_schema::<TaskPauseToolInput>()
}

fn task_resume_schema() -> JsonValue {
    tool_input_schema::<TaskResumeToolInput>()
}

fn tool_input_schema<T>() -> JsonValue
where
    T: JsonSchema,
{
    let mut schema = serde_json::to_value(schema_for!(T)).expect("tool schema should serialize");
    if let Some(object) = schema.as_object_mut() {
        object.remove("$schema");
    }
    schema
}

fn decode_tool_args<T>(invocation: ToolInvocation) -> Result<T, ToolError>
where
    T: for<'de> Deserialize<'de>,
{
    let tool_name = invocation.tool_name.clone();
    let arguments = match invocation.payload {
        ToolPayload::Function { arguments } => arguments,
        ToolPayload::Custom { input } => serde_json::from_str(&input).map_err(|error| {
            ToolError::invalid_arguments(format!(
                "failed to parse custom input for `{tool_name}`: {error}. {}",
                task_tool_argument_hint(tool_name.as_str())
            ))
        })?,
        other => {
            return Err(ToolError::invalid_arguments(format!(
                "`{tool_name}` requires function arguments, got {}",
                other.log_payload(),
            )));
        }
    };
    let schema = task_tool_schema_for_name(tool_name.as_str());
    let arguments = normalize_tool_arguments_from_schema(arguments, &schema)
        .map_err(|error| {
            ToolError::invalid_arguments(format!(
                "{error}. {}",
                task_tool_argument_hint(tool_name.as_str())
            ))
        })?
        .arguments;
    serde_json::from_value(arguments).map_err(|error| {
        ToolError::invalid_arguments(format!(
            "invalid arguments for `{tool_name}`: {error}. {}",
            task_tool_argument_hint(tool_name.as_str())
        ))
    })
}

fn task_tool_schema_for_name(tool_name: &str) -> JsonValue {
    match tool_name {
        TASK_CREATE_TOOL => task_create_schema(),
        TASK_WAIT_TOOL => task_wait_schema(),
        TASK_ACCEPT_TOOL => task_accept_schema(),
        TASK_REVISE_TOOL => task_revise_schema(),
        TASK_CANCEL_TOOL => task_cancel_schema(),
        TASK_UPDATE_TOOL => task_update_schema(),
        TASK_DETACH_TOOL | TASK_GET_TOOL => task_id_schema(),
        TASK_LIST_TOOL => task_list_schema(),
        TASK_RESCHEDULE_TOOL => task_reschedule_schema(),
        TASK_PAUSE_TOOL => task_pause_schema(),
        TASK_RESUME_TOOL => task_resume_schema(),
        _ => json!({ "type": "object" }),
    }
}

fn task_tool_argument_hint(tool_name: &str) -> &'static str {
    match tool_name {
        TASK_CREATE_TOOL => {
            "Expected fields: title, goal, instructions, inputText or input, outputInstructions, and optional trigger. For cron set trigger.kind to cron, trigger.cronExpr to 0 7 * * *, and trigger.timezone to Europe/Moscow. Do not use trigger.spec, trigger.schedule, or top-level cron/timezone."
        }
        TASK_RESCHEDULE_TOOL => {
            "Expected fields: taskId and trigger. For scheduled_at set trigger.scheduledAt to a Unix timestamp such as 1893456000 and optional trigger.timezone to UTC. For cron set trigger.cronExpr to 0 7 * * * and trigger.timezone to Europe/Moscow. Do not use trigger.spec."
        }
        TASK_UPDATE_TOOL => {
            "Expected field: taskId plus at least one patch field such as instructions, inputText, input, outputInstructions, or trigger. For cron set trigger.cronExpr to 0 7 * * * and trigger.timezone to Europe/Moscow. Do not use trigger.spec. Omitted fields stay unchanged; use clear* flags to remove optional values."
        }
        TASK_WAIT_TOOL => {
            "Expected fields: taskIds or runIds, with optional timeoutMs. Use taskIds or runIds arrays, not a single taskId. Example timeoutMs value: 180000."
        }
        TASK_ACCEPT_TOOL => {
            "Expected fields: taskId, runId, candidateId, and optional reason. Use candidate ids returned by task_wait reviewRequired."
        }
        TASK_REVISE_TOOL => {
            "Expected fields: taskId, runId, candidateId, and feedback. Use candidate ids returned by task_wait reviewRequired and provide concrete fix instructions."
        }
        TASK_CANCEL_TOOL | TASK_DETACH_TOOL | TASK_GET_TOOL | TASK_PAUSE_TOOL
        | TASK_RESUME_TOOL => {
            "Expected field: taskId. The value must be a 21-character Pioneer entity id."
        }
        TASK_LIST_TOOL => {
            "Expected optional filters such as ownerKind, status, and limit. For current thread tasks, use ownerKind=thread and omit ownerId. Example status value: running. Example limit value: 20."
        }
        _ => "Check the tool schema and use the documented camelCase fields.",
    }
}

fn normalize_task_list_owner_id(
    owner_kind: Option<TaskOwnerKind>,
    owner_id: Option<String>,
    current_thread_id: &str,
) -> Result<Option<String>, ToolError> {
    match (owner_kind, owner_id) {
        (Some(TaskOwnerKind::Thread), None) => Ok(Some(current_thread_id.to_owned())),
        (None, Some(_)) => Err(ToolError::invalid_arguments(
            "`ownerId` requires `ownerKind`; omit ownerId for current workspace tasks, or use ownerKind=thread without ownerId for the current thread",
        )),
        (_, owner_id) => Ok(owner_id),
    }
}

fn validate_id_list(values: Vec<String>, field: &str) -> Result<Vec<String>, ToolError> {
    values
        .into_iter()
        .map(|value| validate_entity_id(value, field))
        .collect()
}

fn validate_entity_id(value: String, field: &str) -> Result<String, ToolError> {
    let value = required_tool_string(Some(value.as_str()), field)?;
    let char_count = value.chars().count();
    if char_count != 21 {
        return Err(ToolError::invalid_arguments(format!(
            "`{field}` must be a Pioneer entity id with exactly 21 characters, got {char_count}"
        )));
    }
    Ok(value)
}

fn validate_candidate_id(value: String, field: &str) -> Result<String, ToolError> {
    let value = required_tool_string(Some(value.as_str()), field)?;
    let char_count = value.chars().count();
    if char_count > 128 {
        return Err(ToolError::invalid_arguments(format!(
            "`{field}` must be at most 128 characters, got {char_count}"
        )));
    }
    Ok(value)
}

fn validate_optional_entity_id(
    value: Option<String>,
    field: &str,
) -> Result<Option<String>, ToolError> {
    value
        .map(|value| validate_entity_id(value, field))
        .transpose()
}

fn clean_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn normalize_task_instructions(
    instructions: Vec<String>,
    trigger_kind: TaskTriggerKind,
) -> Result<Vec<String>, ToolError> {
    let mut instructions = instructions
        .into_iter()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    dedup_preserve_order(&mut instructions);

    if !requires_durable_agent_prompt(trigger_kind) {
        if instructions.is_empty() {
            instructions.push("Return a concise final result.".to_owned());
        }
        return Ok(instructions);
    }

    if instructions.is_empty() {
        return Err(ToolError::invalid_arguments(
            "`instructions` is required for scheduled, interval, and cron task_create calls; include self-contained future-run steps, runtime capability selection guidance, and failure conditions",
        ));
    }
    if instructions.len() == 1 && instructions[0] == "Return a concise final result." {
        return Err(ToolError::invalid_arguments(
            "`instructions` for scheduled, interval, and cron task_create calls cannot use the concise default; provide a self-contained future-run prompt",
        ));
    }

    instructions.insert(
        0,
        "This is a durable task that may run later. On each run, use the current date/time and currently available tools, skills, MCP servers, and built-ins by capability. Do not rely on hidden task-creation chat context or on tool/skill names that are unavailable at run time. If required capability or data is unavailable, fail clearly instead of fabricating a result."
            .to_owned(),
    );
    dedup_preserve_order(&mut instructions);
    Ok(instructions)
}

fn normalize_task_output_instructions(
    output_instructions: Option<String>,
    trigger_kind: TaskTriggerKind,
) -> Result<Option<String>, ToolError> {
    let output_instructions = clean_optional_string(output_instructions);
    if requires_durable_agent_prompt(trigger_kind) && output_instructions.is_none() {
        return Err(ToolError::invalid_arguments(
            "`outputInstructions` is required for scheduled, interval, and cron task_create calls; specify final result format and failure reporting format",
        ));
    }
    Ok(output_instructions)
}

fn merge_task_agent_input(
    input_text: Option<String>,
    input: Option<TaskAgentInput>,
) -> Result<Option<TaskAgentInput>, ToolError> {
    let input_text = input_text
        .map(|value| required_tool_string(Some(value.as_str()), "inputText"))
        .transpose()?;
    let mut input = input.map(normalize_task_agent_input);
    if let Some(input_text) = input_text {
        match input.as_mut() {
            Some(input) => match input.text.as_deref() {
                Some(existing) if existing == input_text => {}
                Some(existing) => {
                    return Err(ToolError::invalid_arguments(format!(
                        "`inputText` conflicts with `input.text`: got different values `{input_text}` and `{existing}`"
                    )));
                }
                None => input.text = Some(input_text),
            },
            None => {
                input = Some(TaskAgentInput {
                    text: Some(input_text),
                    variables: Vec::new(),
                    attachments: Vec::new(),
                    references: Vec::new(),
                });
            }
        }
    }
    Ok(input)
}

fn normalize_task_agent_input(mut input: TaskAgentInput) -> TaskAgentInput {
    input.text = input
        .text
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    input
}

fn requires_durable_agent_prompt(kind: TaskTriggerKind) -> bool {
    matches!(
        kind,
        TaskTriggerKind::ScheduledAt | TaskTriggerKind::Interval | TaskTriggerKind::Cron
    )
}

fn dedup_preserve_order(values: &mut Vec<String>) {
    let mut seen = BTreeSet::new();
    values.retain(|value| seen.insert(value.clone()));
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
    if let Some(binding) = processor
        .crud_store
        .get_task_run_thread_binding_by_thread(context.thread_id.as_str())
        .await?
        && binding.binding_kind == TaskRunThreadBindingKind::PrimaryExecutor
    {
        return Ok(Some(binding.task_id));
    }

    Ok(None)
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

async fn current_turn_permission_cap(
    processor: &Arc<MessageProcessor>,
    context: &TaskTurnContext,
) -> Result<pioneer_protocol::TurnPermissionProfileCap, ToolError> {
    let turn = processor
        .crud_store
        .get_turn(context.thread_id.as_str(), context.turn_id.as_str())
        .await
        .map_err(|error| ToolError::execution_failed(format!("{error:#}")))?;

    let Some((_, turn)) = turn else {
        return Err(ToolError::execution_failed(format!(
            "turn `{}`/`{}` is missing while resolving task permission cap",
            context.thread_id, context.turn_id
        )));
    };

    let profile = turn
        .permission_profile
        .unwrap_or_else(pioneer_protocol::default_turn_permission_profile_snapshot);
    Ok(pioneer_protocol::task_permission_cap_from_snapshot(
        &profile,
    ))
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
        let input = serde_json::from_value::<TaskWaitToolInput>(arguments.clone()).ok()?;
        let params = input.into_params().ok()?;
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
    review_required_count: u32,
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
        review_required_count: json_u32(&wait_result, "reviewRequiredCount"),
    })
}

fn duplicate_wait_should_block(
    prior: &[PriorWaitCall],
    terminal_count: u32,
    pending_count: u32,
    review_required_count: u32,
) -> bool {
    if prior.is_empty() || pending_count == 0 || review_required_count > 0 {
        return false;
    }
    let last = prior
        .last()
        .expect("prior wait list checked as non-empty before last()");
    if terminal_count > last.terminal_count || review_required_count > last.review_required_count {
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

fn mutation_idempotency_key_for_item(
    tool_name: &str,
    item_id: &str,
    arguments: &JsonValue,
) -> Option<String> {
    arguments
        .get("idempotencyKey")
        .or_else(|| arguments.get("idempotency_key"))
        .and_then(JsonValue::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .or_else(|| Some(format!("{tool_name}:{item_id}")))
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

    let waitable = task_create_response_waitable(response);

    json!({
        "taskId": response.task.id,
        "runId": response.run.as_ref().map(|run| run.id.clone()),
        "waitable": waitable,
        "waitRecommendation": task_create_wait_recommendation(response, waitable),
        "status": task_status_label(response.task.status),
        "title": response.task.title,
        "attachment": attachment,
        "trigger": task_trigger_model_output(&response.trigger),
        "triggerKind": trigger_kind_label(response.trigger.kind()),
        "nextFireAt": response.trigger.next_fire_at,
        "validationHints": task_create_validation_hints(response, waitable),
        "depth": anchor.depth,
        "maxDepth": anchor.max_depth,
        "childThreadId": anchor.child_thread_id,
        "childTurnId": anchor.child_turn_id,
    })
}

fn task_accept_tool_output(response: &TaskAcceptResponse, final_answer_allowed: bool) -> JsonValue {
    json!({
        "accepted": response.accepted,
        "alreadyAccepted": response.already_accepted,
        "taskId": response.task.id,
        "runId": response.run.id,
        "candidateId": response.candidate.id,
        "status": task_status_label(response.task.status),
        "runStatus": run_status_label(response.run.status),
        "candidateStatus": candidate_status_label(response.candidate.status),
        "reviewEventId": response.review_event.id,
        "reviewerKind": reviewer_kind_label(response.review_event.reviewer_kind),
        "summary": response.result.summary,
        "result": response.result,
        "childThreadId": response.child_thread_id,
        "childTurnId": response.child_turn_id,
        "taskTerminal": response.task.status.is_terminal(),
        "finalAnswerAllowed": final_answer_allowed,
    })
}

fn task_revise_tool_output(response: &TaskReviseResponse) -> JsonValue {
    json!({
        "requested": response.requested,
        "alreadyRequested": response.already_requested,
        "taskId": response.task.id,
        "runId": response.run.id,
        "candidateId": response.candidate.id,
        "status": task_status_label(response.task.status),
        "runStatus": run_status_label(response.run.status),
        "candidateStatus": candidate_status_label(response.candidate.status),
        "reviewEventId": response.review_event.id,
        "revisionTaskRunTurnId": response.task_run_turn.id,
        "round": response.round,
        "childThreadId": response.child_thread_id,
        "childTurnId": response.child_turn_id,
        "feedback": response.feedback,
        "additionalInstructions": response.additional_instructions,
        "waitForRevision": true,
    })
}

fn task_trigger_model_output(trigger: &TaskTrigger) -> JsonValue {
    strip_json_nulls(match &trigger.spec {
        TaskTriggerSpec::Immediate => json!({
            "kind": "immediate",
        }),
        TaskTriggerSpec::ScheduledAt {
            scheduled_at,
            timezone,
            catch_up_policy,
        } => json!({
            "kind": "scheduled_at",
            "scheduledAt": scheduled_at,
            "timezone": timezone,
            "catchUpPolicy": catch_up_policy,
        }),
        TaskTriggerSpec::Interval {
            interval_seconds,
            interval_anchor_at,
            catch_up_policy,
        } => json!({
            "kind": "interval",
            "intervalSeconds": interval_seconds,
            "intervalAnchorAt": interval_anchor_at,
            "catchUpPolicy": catch_up_policy,
        }),
        TaskTriggerSpec::Cron {
            cron_expr,
            timezone,
            catch_up_policy,
        } => json!({
            "kind": "cron",
            "cronExpr": cron_expr,
            "timezone": timezone,
            "catchUpPolicy": catch_up_policy,
        }),
        TaskTriggerSpec::Manual { allowed_actor } => json!({
            "kind": "manual",
            "allowedActor": allowed_actor,
        }),
        TaskTriggerSpec::External {
            source,
            event_type,
            filter,
        } => json!({
            "kind": "external",
            "source": source,
            "eventType": event_type,
            "filter": filter,
        }),
        TaskTriggerSpec::Dependency { policy } => json!({
            "kind": "dependency",
            "policy": policy,
        }),
    })
}

fn task_trigger_details_output(triggers: &[TaskTrigger]) -> Vec<JsonValue> {
    triggers.iter().map(task_trigger_detail_output).collect()
}

fn task_trigger_detail_output(trigger: &TaskTrigger) -> JsonValue {
    let mut value = task_trigger_model_output(trigger);
    let Some(object) = value.as_object_mut() else {
        return value;
    };
    object.insert(
        "triggerId".to_owned(),
        JsonValue::String(trigger.id.clone()),
    );
    object.insert(
        "taskId".to_owned(),
        JsonValue::String(trigger.task_id.clone()),
    );
    object.insert(
        "status".to_owned(),
        serde_json::to_value(trigger.status).unwrap_or(JsonValue::Null),
    );
    if let Some(next_fire_at) = trigger.next_fire_at {
        object.insert("nextFireAt".to_owned(), JsonValue::from(next_fire_at));
    }
    if let Some(last_fire_at) = trigger.last_fire_at {
        object.insert("lastFireAt".to_owned(), JsonValue::from(last_fire_at));
    }
    object.insert("createdAt".to_owned(), JsonValue::from(trigger.created_at));
    object.insert("updatedAt".to_owned(), JsonValue::from(trigger.updated_at));
    value
}

fn strip_json_nulls(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(object) => JsonValue::Object(
            object
                .into_iter()
                .filter_map(|(key, value)| {
                    let value = strip_json_nulls(value);
                    (!value.is_null()).then_some((key, value))
                })
                .collect(),
        ),
        JsonValue::Array(items) => {
            JsonValue::Array(items.into_iter().map(strip_json_nulls).collect())
        }
        other => other,
    }
}

fn task_create_validation_hints(
    response: &TaskCreateResponse,
    waitable: bool,
) -> Vec<&'static str> {
    let mut hints = Vec::new();
    if matches!(
        response.trigger.kind(),
        TaskTriggerKind::ScheduledAt | TaskTriggerKind::Interval | TaskTriggerKind::Cron
    ) {
        hints.push("scheduled_task_has_no_active_run_until_due");
        hints.push("do_not_call_task_wait_when_waitable_false");
        hints.push("future_run_prompt_must_be_self_contained");
    }
    if waitable {
        hints.push("active_attached_task_should_be_joined_before_final_answer");
    }
    hints
}

fn task_create_response_waitable(response: &TaskCreateResponse) -> bool {
    response
        .run
        .as_ref()
        .is_some_and(|run| !run.status.is_terminal())
        || matches!(
            response.task.status,
            TaskStatus::Queued | TaskStatus::Running | TaskStatus::Waiting
        )
}

fn task_create_wait_recommendation(response: &TaskCreateResponse, waitable: bool) -> &'static str {
    if waitable {
        return "call_task_wait_for_active_attached_work_before_final_answer";
    }
    match response.trigger.kind() {
        TaskTriggerKind::ScheduledAt | TaskTriggerKind::Interval | TaskTriggerKind::Cron => {
            "do_not_call_task_wait_confirm_schedule"
        }
        _ => "do_not_call_task_wait_unless_an_active_run_exists",
    }
}

fn task_update_tool_output(response: &TaskUpdateResponse) -> JsonValue {
    json!({
        "task": task_summary(&response.task),
        "changedFields": &response.changed_fields,
        "trigger": response.trigger.as_ref().map(task_trigger_detail_output),
        "agentSpec": &response.agent_spec,
        "revision": response.task.revision,
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
        "reviewRequiredCount": response.review_required_count,
        "blockedCount": response.blocked_count,
        "completed": response.completed.iter().map(wait_item_output).collect::<Vec<_>>(),
        "failed": response.failed.iter().map(wait_item_output).collect::<Vec<_>>(),
        "blocked": response.blocked.iter().map(wait_item_output).collect::<Vec<_>>(),
        "cancelled": response.cancelled.iter().map(wait_item_output).collect::<Vec<_>>(),
        "reviewRequired": response.review_required.iter().map(review_required_item_output).collect::<Vec<_>>(),
        "pending": response.pending.iter().map(wait_item_output).collect::<Vec<_>>(),
        "nonWaitable": response.non_waitable.iter().map(non_waitable_item_output).collect::<Vec<_>>(),
        "nonWaitableCount": response.non_waitable_count,
        "timedOut": response.timed_out,
    })
}

fn task_wait_guard_output(
    signature: &TaskWaitSignature,
    previous_wait_item_id: &str,
    total_count: u32,
    terminal_count: u32,
    pending_count: u32,
    review_required_count: u32,
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
        "reviewRequiredCount": review_required_count,
        "previousTimedOut": previous_timed_out,
        "priorWaitCount": prior_wait_count,
        "recommendation": if previous_timed_out {
            "cancel_detach_or_return_partial_result"
        } else {
            "wait_for_timeline_change_or_cancel"
        },
    })
}

fn task_wait_non_waitable_output(
    signature: &TaskWaitSignature,
    non_waitable: Vec<JsonValue>,
    waitable_task_ids: Vec<String>,
    has_run_targets: bool,
) -> JsonValue {
    let recommendation = if waitable_task_ids.is_empty() && !has_run_targets {
        "confirm_scheduled_task"
    } else {
        "call_task_wait_again_only_for_waitable_active_runs"
    };

    json!({
        "waitable": false,
        "reason": "scheduled_task_has_no_active_run",
        "message": "One or more targets are scheduled for future execution and have no active run to wait for. Do not call task_wait for scheduled, interval, or cron tasks after task_create returned waitable=false/runId=null; confirm the schedule instead.",
        "waitSignature": signature.to_json(),
        "nonWaitable": non_waitable,
        "waitableTaskIds": waitable_task_ids,
        "hasRunTargets": has_run_targets,
        "recommendation": recommendation,
        "timedOut": false,
    })
}

fn non_waitable_guard_item_output(item: &pioneer_protocol::TaskWaitNonWaitableItem) -> JsonValue {
    json!({
        "taskId": item.item.task.id,
        "title": item.item.task.title,
        "status": task_status_label(item.item.task.status),
        "triggerKind": null,
        "nextFireAt": item.next_fire_at,
        "runId": null,
        "waitable": false,
        "reason": match item.reason {
            pioneer_protocol::TaskWaitNonWaitableReason::FutureScheduledTaskWithoutActiveRun => {
                "future_scheduled_task_without_active_run"
            }
        },
    })
}

#[cfg(test)]
fn task_wait_target_is_non_waitable_scheduled(response: &TaskGetResponse, now: i64) -> bool {
    if response.task.status != TaskStatus::Scheduled {
        return false;
    }
    if response.runs.iter().any(|run| !run.status.is_terminal()) {
        return false;
    }
    response.triggers.iter().any(|trigger| {
        trigger.status == pioneer_protocol::TaskTriggerStatus::Active
            && matches!(
                trigger.kind(),
                TaskTriggerKind::ScheduledAt | TaskTriggerKind::Interval | TaskTriggerKind::Cron
            )
            && trigger
                .next_fire_at
                .is_some_and(|next_fire_at| next_fire_at > now)
    })
}

#[cfg(test)]
fn non_waitable_scheduled_task_output(response: &TaskGetResponse, now: i64) -> JsonValue {
    let trigger = response
        .triggers
        .iter()
        .rev()
        .find(|trigger| {
            trigger.status == pioneer_protocol::TaskTriggerStatus::Active
                && trigger
                    .next_fire_at
                    .is_some_and(|next_fire_at| next_fire_at > now)
        })
        .or_else(|| response.triggers.last());

    json!({
        "taskId": response.task.id,
        "title": response.task.title,
        "status": task_status_label(response.task.status),
        "triggerKind": trigger.map(|trigger| trigger_kind_label(trigger.kind())),
        "nextFireAt": trigger.and_then(|trigger| trigger.next_fire_at),
        "runId": null,
        "waitable": false,
        "reason": "future_scheduled_task_without_active_run",
    })
}

fn wait_mode_label(mode: pioneer_protocol::TaskWaitMode) -> &'static str {
    match mode {
        pioneer_protocol::TaskWaitMode::AllTerminal => "all_terminal",
        pioneer_protocol::TaskWaitMode::AnyTerminal => "any_terminal",
        pioneer_protocol::TaskWaitMode::AllTerminalOrReviewRequired => {
            "all_terminal_or_review_required"
        }
        pioneer_protocol::TaskWaitMode::AnyTerminalOrReviewRequired => {
            "any_terminal_or_review_required"
        }
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

fn review_required_item_output(item: &pioneer_protocol::TaskWaitReviewItem) -> JsonValue {
    let run = item.item.run.as_ref();
    let candidate = &item.candidate;
    let review_mode = item
        .review_policy
        .as_ref()
        .map(|policy| review_mode_label(policy.mode));
    let user_approval_required = item
        .review_policy
        .as_ref()
        .is_some_and(|policy| policy.mode == pioneer_protocol::TaskAgentReviewMode::UserApproval);
    let summary = candidate.summary.clone().or_else(|| {
        candidate
            .result
            .as_ref()
            .and_then(|result| result.summary.clone())
    });
    let allowed_actions = item
        .allowed_actions
        .iter()
        .copied()
        .map(wait_review_action_label)
        .collect::<Vec<_>>();
    json!({
        "taskId": item.item.task.id,
        "runId": run.map(|run| run.id.clone()).unwrap_or_else(|| candidate.run_id.clone()),
        "title": item.item.task.title,
        "status": wait_item_status(&item.item.task, run),
        "candidateId": candidate.id,
        "candidateStatus": candidate_status_label(candidate.status),
        "reviewMode": review_mode,
        "userApprovalRequired": user_approval_required,
        "reviewPolicy": item.review_policy,
        "round": candidate.round,
        "summary": summary,
        "resultPreview": result_preview(candidate.result.as_ref()),
        "result": candidate.result,
        "extractionError": candidate.extraction_error,
        "extractionErrorPreview": error_preview(candidate.extraction_error.as_ref()),
        "diagnostics": candidate.diagnostics,
        "childThreadId": item.item.child_thread_id,
        "childTurnId": item.item.child_turn_id,
        "permissionMode": item.item.permission_profile.as_ref().map(|profile| profile.mode.as_str()),
        "permissionSource": item.item.permission_profile.as_ref().map(|profile| profile.source.as_str()),
        "maxRevisionRounds": item.max_revision_rounds,
        "remainingRevisionRounds": item.remaining_revision_rounds,
        "allowedActions": allowed_actions,
        "revisionBlockedReason": item.revision_blocked_reason.map(wait_revision_blocked_reason_label),
        "recommendation": review_required_recommendation(item),
    })
}

fn review_required_task_observation(
    item: &pioneer_protocol::TaskWaitReviewItem,
) -> ReviewRequiredTaskObservation {
    let run = item.item.run.as_ref();
    let candidate = &item.candidate;
    let summary = candidate.summary.as_deref().or_else(|| {
        candidate
            .result
            .as_ref()
            .and_then(|result| result.summary.as_deref())
    });
    ReviewRequiredTaskObservation {
        task_id: item.item.task.id.clone(),
        run_id: run
            .map(|run| run.id.clone())
            .unwrap_or_else(|| candidate.run_id.clone()),
        candidate_id: candidate.id.clone(),
        title: bounded_preview(item.item.task.title.as_str(), 160),
        status: wait_item_status(&item.item.task, run),
        candidate_status: candidate_status_label(candidate.status),
        round: candidate.round,
        summary: summary.map(|summary| bounded_preview(summary, 240)),
        result_preview: result_preview(candidate.result.as_ref()),
        extraction_error_preview: error_preview(candidate.extraction_error.as_ref()),
        diagnostics: bounded_diagnostics(candidate.diagnostics.as_slice()),
        child_thread_id: item.item.child_thread_id.clone(),
        child_turn_id: item.item.child_turn_id.clone(),
        max_revision_rounds: item.max_revision_rounds,
        remaining_revision_rounds: item.remaining_revision_rounds,
        allowed_actions: item
            .allowed_actions
            .iter()
            .copied()
            .map(wait_review_action_label)
            .map(str::to_owned)
            .collect(),
        revision_blocked_reason: item
            .revision_blocked_reason
            .map(wait_revision_blocked_reason_label)
            .map(str::to_owned),
    }
}

fn non_waitable_item_output(item: &pioneer_protocol::TaskWaitNonWaitableItem) -> JsonValue {
    json!({
        "item": wait_item_output(&item.item),
        "reason": match item.reason {
            pioneer_protocol::TaskWaitNonWaitableReason::FutureScheduledTaskWithoutActiveRun => {
                "future_scheduled_task_without_active_run"
            }
        },
        "nextFireAt": item.next_fire_at,
    })
}

fn task_get_legacy_lineage_output(response: &TaskGetResponse) -> Vec<JsonValue> {
    response
        .task_run_thread_bindings
        .iter()
        .filter(|binding| binding.binding_kind == TaskRunThreadBindingKind::PrimaryExecutor)
        .filter_map(|binding| {
            let lineage = response
                .thread_lineage
                .iter()
                .find(|lineage| lineage.child_thread_id == binding.thread_id)?;
            let child_turn_id = task_get_legacy_child_turn_id(response, binding);
            Some(json!({
                "childThreadId": lineage.child_thread_id.clone(),
                "childTurnId": child_turn_id,
                "parentThreadId": lineage.parent_thread_id.clone(),
                "parentTurnId": lineage.created_by_turn_id.clone(),
                "taskId": binding.task_id.clone(),
                "taskRunId": binding.run_id.clone(),
                "rootThreadId": lineage.root_thread_id.clone(),
                "depth": lineage.depth,
                "createdAt": lineage.created_at,
            }))
        })
        .collect()
}

fn task_get_legacy_child_turn_id(
    response: &TaskGetResponse,
    binding: &pioneer_protocol::TaskRunThreadBinding,
) -> Option<String> {
    response
        .result_candidates
        .iter()
        .rev()
        .find(|candidate| {
            candidate.run_id == binding.run_id
                && candidate.thread_id == binding.thread_id
                && candidate.status == TaskResultCandidateStatus::Accepted
        })
        .map(|candidate| candidate.task_run_turn_id.as_str())
        .and_then(|task_run_turn_id| {
            response
                .task_run_turns
                .iter()
                .find(|turn| turn.id == task_run_turn_id)
        })
        .or_else(|| {
            response
                .task_run_turns
                .iter()
                .filter(|turn| turn.run_id == binding.run_id && turn.thread_id == binding.thread_id)
                .max_by(|left, right| {
                    left.sequence
                        .cmp(&right.sequence)
                        .then_with(|| left.created_at.cmp(&right.created_at))
                })
        })
        .map(|turn| turn.turn_id.clone())
}

fn wait_item_status(task: &Task, run: Option<&TaskRun>) -> String {
    if let Some(run) = run {
        return run_status_label(run.status);
    }
    task_status_label(task.status)
}

fn candidate_status_label(status: TaskResultCandidateStatus) -> String {
    serde_json::to_value(status)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{status:?}").to_ascii_lowercase())
}

fn reviewer_kind_label(kind: TaskResultReviewerKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{kind:?}").to_ascii_lowercase())
}

fn review_mode_label(mode: pioneer_protocol::TaskAgentReviewMode) -> String {
    serde_json::to_value(mode)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| format!("{mode:?}").to_ascii_lowercase())
}

fn wait_review_action_label(action: pioneer_protocol::TaskWaitReviewAction) -> &'static str {
    match action {
        pioneer_protocol::TaskWaitReviewAction::TaskAccept => "task_accept",
        pioneer_protocol::TaskWaitReviewAction::TaskRevise => "task_revise",
        pioneer_protocol::TaskWaitReviewAction::TaskCancel => "task_cancel",
    }
}

fn wait_revision_blocked_reason_label(
    reason: pioneer_protocol::TaskWaitRevisionBlockedReason,
) -> &'static str {
    match reason {
        pioneer_protocol::TaskWaitRevisionBlockedReason::MaxRevisionRoundsReached => {
            "max_revision_rounds_reached"
        }
        pioneer_protocol::TaskWaitRevisionBlockedReason::CandidateNotRevisable => {
            "candidate_not_revisable"
        }
    }
}

fn review_required_recommendation(item: &pioneer_protocol::TaskWaitReviewItem) -> &'static str {
    let can_accept = item
        .allowed_actions
        .contains(&pioneer_protocol::TaskWaitReviewAction::TaskAccept);
    let can_revise = item
        .allowed_actions
        .contains(&pioneer_protocol::TaskWaitReviewAction::TaskRevise);
    match (can_accept, can_revise) {
        (true, true) => "call_task_accept_or_task_revise",
        (true, false) => "call_task_accept_or_task_cancel",
        (false, true) => "call_task_revise_or_task_cancel",
        (false, false) => "call_task_cancel",
    }
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
    let run = select_task_anchor_run(response);
    task_turn_item_from_response_with_run(
        processor,
        response,
        run,
        task_anchor_id(response.task.id.as_str()),
    )
    .await
}

pub(crate) async fn task_turn_item_from_response_for_run(
    processor: &MessageProcessor,
    response: &TaskGetResponse,
    run_id: &str,
    item_id: String,
) -> anyhow::Result<TaskTurnItem> {
    let run = response.runs.iter().find(|run| run.id == run_id);
    task_turn_item_from_response_with_run(processor, response, run, item_id).await
}

pub(crate) fn task_anchor_id(task_id: &str) -> String {
    format!("task_{task_id}")
}

pub(crate) fn task_run_anchor_id(run_id: &str) -> String {
    format!("task_run_{run_id}")
}

async fn task_turn_item_from_response_with_run(
    processor: &MessageProcessor,
    response: &TaskGetResponse,
    run: Option<&TaskRun>,
    item_id: String,
) -> anyhow::Result<TaskTurnItem> {
    let task = &response.task;
    let trigger = run
        .and_then(|run| {
            run.trigger_id.as_ref().and_then(|trigger_id| {
                response
                    .triggers
                    .iter()
                    .find(|trigger| trigger.id == *trigger_id)
            })
        })
        .or_else(|| response.triggers.last());
    let agent_spec = select_anchor_agent_spec(response, run);
    let child_anchor = match run {
        Some(run) => {
            processor
                .crud_store
                .get_task_run_child_anchor(run.id.as_str())
                .await?
        }
        None => Default::default(),
    };
    Ok(TaskTurnItem {
        id: item_id,
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
        child_thread_id: child_anchor.child_thread_id,
        child_turn_id: child_anchor.child_turn_id,
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

fn select_task_anchor_run(response: &TaskGetResponse) -> Option<&TaskRun> {
    let run = response.runs.last()?;
    task_run_uses_task_anchor(response, run).then_some(run)
}

fn task_run_uses_task_anchor(response: &TaskGetResponse, run: &TaskRun) -> bool {
    let attached = response
        .task
        .lifecycle_policy
        .as_ref()
        .map(|policy| policy.attachment == TaskAttachmentMode::Attached)
        .unwrap_or(false);
    if !attached {
        return false;
    }

    run.trigger_id
        .as_ref()
        .and_then(|trigger_id| {
            response
                .triggers
                .iter()
                .find(|trigger| trigger.id == *trigger_id)
        })
        .map(|trigger| trigger.kind() == TaskTriggerKind::Immediate)
        .unwrap_or(false)
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

fn bounded_diagnostics(values: &[String]) -> Vec<String> {
    values
        .iter()
        .take(5)
        .map(|value| bounded_preview(value, 240))
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn prior(timed_out: bool, terminal_count: u32) -> PriorWaitCall {
        PriorWaitCall {
            item_id: "wait_item".to_owned(),
            timed_out,
            terminal_count,
            review_required_count: 0,
        }
    }

    fn prior_with_review(
        timed_out: bool,
        terminal_count: u32,
        review_required_count: u32,
    ) -> PriorWaitCall {
        PriorWaitCall {
            item_id: "wait_item".to_owned(),
            timed_out,
            terminal_count,
            review_required_count,
        }
    }

    #[test]
    fn domain_map_matches_task_tool_specs() {
        let specs = task_tool_specs();
        let actual = specs
            .iter()
            .map(|configured| configured.spec.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            actual.as_slice(),
            pioneer_tools::BuiltinToolDomain::Task.tool_names()
        );
    }

    #[test]
    fn task_wait_has_long_wall_clock_recovery_budget() {
        let specs = task_tool_specs();
        let task_wait = specs
            .iter()
            .find(|configured| configured.spec.name == TASK_WAIT_TOOL)
            .expect("task_wait spec should exist");

        assert_eq!(task_wait.spec.recovery.max_wall_clock_secs, Some(3_600));
    }

    fn sample_task(status: TaskStatus) -> Task {
        Task {
            id: "task_1234567890123456".to_owned(),
            workspace_id: "workspace_12345678901".to_owned(),
            owner_kind: TaskOwnerKind::Workspace,
            owner_id: Some("workspace_12345678901".to_owned()),
            created_by_thread_id: Some("thread_12345678901234".to_owned()),
            created_by_turn_id: Some("turn_123456789012345".to_owned()),
            root_task_id: None,
            parent_task_id: None,
            executor_kind: TaskExecutorKind::Agent,
            status,
            title: "Daily Weather Forecast".to_owned(),
            goal: "Send weather".to_owned(),
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
            created_at: 100,
            updated_at: 100,
            completed_at: None,
        }
    }

    fn sample_cron_trigger(next_fire_at: Option<i64>) -> TaskTrigger {
        TaskTrigger {
            id: "trigger_1234567890123".to_owned(),
            task_id: "task_1234567890123456".to_owned(),
            status: pioneer_protocol::TaskTriggerStatus::Active,
            spec: TaskTriggerSpec::Cron {
                cron_expr: "0 7 * * *".to_owned(),
                timezone: "Europe/Moscow".to_owned(),
                catch_up_policy: None,
            },
            next_fire_at,
            last_fire_at: None,
            created_at: 100,
            updated_at: 100,
        }
    }

    fn sample_run(status: TaskRunStatus) -> TaskRun {
        TaskRun {
            id: "run_12345678901234567".to_owned(),
            task_id: "task_1234567890123456".to_owned(),
            trigger_id: Some("trigger_1234567890123".to_owned()),
            parent_run_id: None,
            run_group_id: "group_123456789012345".to_owned(),
            attempt_number: 1,
            retry_of_run_id: None,
            ready_at: Some(100),
            run_number: 1,
            status,
            executor_kind: TaskExecutorKind::Agent,
            started_at: None,
            completed_at: None,
            heartbeat_at: None,
            locked_by: None,
            lock_expires_at: None,
            result: None,
            error: None,
            created_at: 100,
            updated_at: 100,
        }
    }

    fn sample_review_candidate(
        status: TaskResultCandidateStatus,
        round: u32,
    ) -> pioneer_protocol::TaskResultCandidate {
        pioneer_protocol::TaskResultCandidate {
            id: "candidate_123456789012".to_owned(),
            task_id: "task_1234567890123456".to_owned(),
            run_id: "run_12345678901234567".to_owned(),
            task_run_turn_id: "task_run_turn_123456".to_owned(),
            thread_id: "thread_child12345678".to_owned(),
            turn_id: "turn_child123456789".to_owned(),
            round,
            status,
            result: (status == TaskResultCandidateStatus::PendingReview).then(|| TaskResult {
                summary: Some("candidate summary".to_owned()),
                data: Some(pioneer_protocol::TaskValue::String(
                    "candidate data".to_owned(),
                )),
                artifacts: Vec::new(),
                completed_by_run_id: Some("run_12345678901234567".to_owned()),
            }),
            extraction_error: (status == TaskResultCandidateStatus::ExtractionFailed).then(|| {
                TaskError {
                    code: "extraction_failed".to_owned(),
                    message: "could not extract task result".to_owned(),
                    class: pioneer_protocol::TaskErrorClass::Validation,
                    details: None,
                    failed_run_id: Some("run_12345678901234567".to_owned()),
                }
            }),
            summary: Some("candidate summary".to_owned()),
            diagnostics: vec!["used fallback text result".to_owned()],
            final_review_event_id: None,
            created_at: 120,
            updated_at: 120,
            resolved_at: None,
        }
    }

    fn sample_review_wait_response(
        allowed_actions: Vec<pioneer_protocol::TaskWaitReviewAction>,
        remaining_revision_rounds: u32,
        revision_blocked_reason: Option<pioneer_protocol::TaskWaitRevisionBlockedReason>,
        candidate_status: TaskResultCandidateStatus,
    ) -> pioneer_protocol::TaskWaitResponse {
        pioneer_protocol::TaskWaitResponse {
            completed: Vec::new(),
            failed: Vec::new(),
            blocked: Vec::new(),
            cancelled: Vec::new(),
            review_required: vec![pioneer_protocol::TaskWaitReviewItem {
                item: pioneer_protocol::TaskWaitItem {
                    task: sample_task(TaskStatus::WaitingReview),
                    run: Some(sample_run(TaskRunStatus::WaitingReview)),
                    child_thread_id: Some("thread_child12345678".to_owned()),
                    child_turn_id: Some("turn_child123456789".to_owned()),
                    permission_profile: None,
                },
                candidate: sample_review_candidate(candidate_status, 2),
                review_policy: Some(
                    pioneer_protocol::TaskAgentReviewPolicy::parent_agent_default(2),
                ),
                max_revision_rounds: 2,
                remaining_revision_rounds,
                allowed_actions,
                revision_blocked_reason,
            }],
            pending: Vec::new(),
            non_waitable: Vec::new(),
            timed_out: false,
            total_count: 1,
            terminal_count: 0,
            pending_count: 0,
            review_required_count: 1,
            blocked_count: 0,
            non_waitable_count: 0,
            mode: TaskWaitMode::AllTerminalOrReviewRequired,
        }
    }

    fn sample_task_response(
        task_status: TaskStatus,
        runs: Vec<TaskRun>,
        next_fire_at: Option<i64>,
    ) -> TaskGetResponse {
        TaskGetResponse {
            task: sample_task(task_status),
            triggers: vec![sample_cron_trigger(next_fire_at)],
            runs,
            agent_specs: Vec::new(),
            dependencies: Vec::new(),
            write_locks: Vec::new(),
            thread_lineage: Vec::new(),
            task_run_thread_bindings: Vec::new(),
            task_run_turns: Vec::new(),
            result_candidates: Vec::new(),
            result_review_events: Vec::new(),
        }
    }

    #[test]
    fn task_get_legacy_lineage_output_is_derived_from_target_rows() {
        let mut response = sample_task_response(
            TaskStatus::Completed,
            vec![sample_run(TaskRunStatus::Succeeded)],
            None,
        );
        let run_id = response.runs[0].id.clone();
        response
            .thread_lineage
            .push(pioneer_protocol::TaskThreadLineage {
                child_thread_id: "child_thread_target".to_owned(),
                parent_thread_id: "parent_thread".to_owned(),
                root_thread_id: "parent_thread".to_owned(),
                depth: 1,
                origin_kind: Some("task_run".to_owned()),
                created_by_thread_id: Some("parent_thread".to_owned()),
                created_by_turn_id: Some("parent_turn".to_owned()),
                created_at: 123,
            });
        response
            .task_run_thread_bindings
            .push(pioneer_protocol::TaskRunThreadBinding {
                id: "binding_target".to_owned(),
                task_id: response.task.id.clone(),
                run_id: run_id.clone(),
                execution_id: None,
                thread_id: "child_thread_target".to_owned(),
                binding_kind: TaskRunThreadBindingKind::PrimaryExecutor,
                created_at: 123,
            });
        response.task_run_turns.push(pioneer_protocol::TaskRunTurn {
            id: "turn_target".to_owned(),
            task_id: response.task.id.clone(),
            run_id,
            execution_id: None,
            thread_id: "child_thread_target".to_owned(),
            turn_id: "child_turn_target".to_owned(),
            kind: pioneer_protocol::TaskRunTurnKind::Initial,
            round: 0,
            sequence: 0,
            status: pioneer_protocol::TaskRunTurnStatus::CandidateCreated,
            reviews_candidate_id: None,
            requested_by_candidate_id: None,
            requested_by_review_event_id: None,
            created_at: 123,
            started_at: Some(123),
            completed_at: Some(124),
        });

        let output = task_get_legacy_lineage_output(&response);

        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["childThreadId"], "child_thread_target");
        assert_eq!(output[0]["childTurnId"], "child_turn_target");
        assert_eq!(output[0]["taskId"], response.task.id);
        assert_eq!(output[0]["taskRunId"], response.runs[0].id);
    }

    #[test]
    fn task_wait_tool_output_renders_review_required_candidate() {
        let response = sample_review_wait_response(
            vec![
                pioneer_protocol::TaskWaitReviewAction::TaskAccept,
                pioneer_protocol::TaskWaitReviewAction::TaskRevise,
                pioneer_protocol::TaskWaitReviewAction::TaskCancel,
            ],
            1,
            None,
            TaskResultCandidateStatus::PendingReview,
        );
        let signature = TaskWaitSignature {
            task_ids: vec!["task_1234567890123456".to_owned()],
            run_ids: Vec::new(),
            mode: TaskWaitMode::AllTerminalOrReviewRequired,
        };

        let output = task_wait_tool_output(&response, &signature);

        assert_eq!(output["reviewRequiredCount"], 1);
        assert_eq!(output["completed"].as_array().unwrap().len(), 0);
        let review = &output["reviewRequired"][0];
        assert_eq!(review["taskId"], "task_1234567890123456");
        assert_eq!(review["runId"], "run_12345678901234567");
        assert_eq!(review["status"], "waiting_review");
        assert_eq!(review["candidateId"], "candidate_123456789012");
        assert_eq!(review["candidateStatus"], "pending_review");
        assert_eq!(review["summary"], "candidate summary");
        assert_eq!(review["childThreadId"], "thread_child12345678");
        assert_eq!(review["childTurnId"], "turn_child123456789");
        assert_eq!(review["remainingRevisionRounds"], 1);
        assert_eq!(
            review["allowedActions"],
            json!(["task_accept", "task_revise", "task_cancel"])
        );
        assert_eq!(review["revisionBlockedReason"], JsonValue::Null);
        assert_eq!(review["recommendation"], "call_task_accept_or_task_revise");
        assert_eq!(review["diagnostics"], json!(["used fallback text result"]));
    }

    #[test]
    fn task_accept_tool_input_accepts_review_candidate_id() {
        let candidate_id = "trc_TBsmD0lIlHbX5Nlhxfz0u_VaygfHZgOuehGCuL7yyow".to_owned();

        let params = TaskAcceptToolInput {
            task_id: "GHN2SSkCdoTJmqCwTTjyI".to_owned(),
            run_id: "TBsmD0lIlHbX5Nlhxfz0u".to_owned(),
            candidate_id: candidate_id.clone(),
            reason: Some("accepted".to_owned()),
            idempotency_key: None,
        }
        .into_params()
        .expect("review candidate ids returned by task_wait should be valid");

        assert_eq!(params.candidate_id, candidate_id);
    }

    #[test]
    fn task_revise_tool_input_accepts_review_candidate_id() {
        let candidate_id = "trc_M502Yo7M0fytkc5sFHMvJ_SyvPgFxzLtOtI9IVQ6eLG".to_owned();

        let params = TaskReviseToolInput {
            task_id: "mssjiLzl12AKQx3FnawmX".to_owned(),
            run_id: "M502Yo7M0fytkc5sFHMvJ".to_owned(),
            candidate_id: candidate_id.clone(),
            feedback: "Add missing details.".to_owned(),
            additional_instructions: Vec::new(),
            idempotency_key: None,
        }
        .into_params()
        .expect("review candidate ids returned by task_wait should be valid");

        assert_eq!(params.candidate_id, candidate_id);
    }

    #[test]
    fn task_accept_tool_output_returns_parent_loop_fields() {
        let result = TaskResult {
            summary: Some("accepted summary".to_owned()),
            data: Some(pioneer_protocol::TaskValue::String(
                "accepted data".to_owned(),
            )),
            artifacts: Vec::new(),
            completed_by_run_id: Some("run_12345678901234567".to_owned()),
        };
        let mut run = sample_run(TaskRunStatus::Succeeded);
        run.result = Some(result.clone());
        let mut candidate = sample_review_candidate(TaskResultCandidateStatus::Accepted, 2);
        candidate.result = Some(result.clone());
        candidate.final_review_event_id = Some("review_accept1234567".to_owned());
        candidate.resolved_at = Some(130);
        let response = TaskAcceptResponse {
            task: sample_task(TaskStatus::Completed),
            run,
            candidate,
            review_event: pioneer_protocol::TaskResultReviewEvent {
                id: "review_accept1234567".to_owned(),
                candidate_id: "candidate_123456789012".to_owned(),
                task_id: "task_1234567890123456".to_owned(),
                run_id: "run_12345678901234567".to_owned(),
                task_run_turn_id: "task_run_turn_123456".to_owned(),
                reviewer_kind: TaskResultReviewerKind::ParentAgent,
                reviewer_thread_id: Some("thread_parent1234567".to_owned()),
                reviewer_turn_id: Some("turn_parent12345678".to_owned()),
                reviewer_user_id: None,
                reviewer_agent_spec_id: None,
                event_kind: pioneer_protocol::TaskResultReviewEventKind::Decision,
                decision: pioneer_protocol::TaskResultReviewDecision::Accept,
                feedback_text: Some("good enough".to_owned()),
                feedback: None,
                confidence: None,
                supersedes_review_event_id: None,
                next_task_run_turn_id: None,
                created_at: 130,
            },
            result,
            accepted: true,
            already_accepted: false,
            status: TaskStatus::Completed,
            child_thread_id: Some("thread_child12345678".to_owned()),
            child_turn_id: Some("turn_child123456789".to_owned()),
        };

        let output = task_accept_tool_output(&response, true);

        assert_eq!(output["accepted"], true);
        assert_eq!(output["alreadyAccepted"], false);
        assert_eq!(output["taskId"], "task_1234567890123456");
        assert_eq!(output["runId"], "run_12345678901234567");
        assert_eq!(output["candidateId"], "candidate_123456789012");
        assert_eq!(output["status"], "completed");
        assert_eq!(output["runStatus"], "succeeded");
        assert_eq!(output["candidateStatus"], "accepted");
        assert_eq!(output["reviewerKind"], "parent_agent");
        assert_eq!(output["summary"], "accepted summary");
        assert_eq!(output["childThreadId"], "thread_child12345678");
        assert_eq!(output["childTurnId"], "turn_child123456789");
        assert_eq!(output["taskTerminal"], true);
        assert_eq!(output["finalAnswerAllowed"], true);
    }

    #[test]
    fn task_accept_tool_output_can_block_final_answer_when_other_tasks_pending() {
        let result = TaskResult {
            summary: Some("accepted summary".to_owned()),
            data: None,
            artifacts: Vec::new(),
            completed_by_run_id: Some("run_12345678901234567".to_owned()),
        };
        let mut run = sample_run(TaskRunStatus::Succeeded);
        run.result = Some(result.clone());
        let mut candidate = sample_review_candidate(TaskResultCandidateStatus::Accepted, 0);
        candidate.result = Some(result.clone());
        let response = TaskAcceptResponse {
            task: sample_task(TaskStatus::Completed),
            run,
            candidate,
            review_event: pioneer_protocol::TaskResultReviewEvent {
                id: "review_accept1234567".to_owned(),
                candidate_id: "candidate_123456789012".to_owned(),
                task_id: "task_1234567890123456".to_owned(),
                run_id: "run_12345678901234567".to_owned(),
                task_run_turn_id: "task_run_turn_123456".to_owned(),
                reviewer_kind: TaskResultReviewerKind::ParentAgent,
                reviewer_thread_id: Some("thread_parent1234567".to_owned()),
                reviewer_turn_id: Some("turn_parent12345678".to_owned()),
                reviewer_user_id: None,
                reviewer_agent_spec_id: None,
                event_kind: pioneer_protocol::TaskResultReviewEventKind::Decision,
                decision: pioneer_protocol::TaskResultReviewDecision::Accept,
                feedback_text: None,
                feedback: None,
                confidence: None,
                supersedes_review_event_id: None,
                next_task_run_turn_id: None,
                created_at: 130,
            },
            result,
            accepted: true,
            already_accepted: false,
            status: TaskStatus::Completed,
            child_thread_id: Some("thread_child12345678".to_owned()),
            child_turn_id: Some("turn_child123456789".to_owned()),
        };

        let output = task_accept_tool_output(&response, false);

        assert_eq!(output["taskTerminal"], true);
        assert_eq!(output["finalAnswerAllowed"], false);
    }

    #[test]
    fn task_wait_tool_output_removes_revise_when_revision_limit_reached() {
        let response = sample_review_wait_response(
            vec![
                pioneer_protocol::TaskWaitReviewAction::TaskAccept,
                pioneer_protocol::TaskWaitReviewAction::TaskCancel,
            ],
            0,
            Some(pioneer_protocol::TaskWaitRevisionBlockedReason::MaxRevisionRoundsReached),
            TaskResultCandidateStatus::PendingReview,
        );
        let signature = TaskWaitSignature {
            task_ids: vec!["task_1234567890123456".to_owned()],
            run_ids: Vec::new(),
            mode: TaskWaitMode::AllTerminalOrReviewRequired,
        };

        let output = task_wait_tool_output(&response, &signature);
        let review = &output["reviewRequired"][0];

        assert_eq!(review["remainingRevisionRounds"], 0);
        assert_eq!(
            review["allowedActions"],
            json!(["task_accept", "task_cancel"])
        );
        assert_eq!(
            review["revisionBlockedReason"],
            "max_revision_rounds_reached"
        );
        assert_eq!(review["recommendation"], "call_task_accept_or_task_cancel");
    }

    fn sample_task_turn_item() -> TaskTurnItem {
        TaskTurnItem {
            id: "task_task_1234567890123456".to_owned(),
            task_id: "task_1234567890123456".to_owned(),
            run_id: None,
            parent_task_id: None,
            root_task_id: None,
            title: "Daily Weather Forecast".to_owned(),
            status: TaskStatus::Scheduled,
            trigger_kind: TaskTriggerKind::Cron,
            executor_kind: TaskExecutorKind::Agent,
            child_thread_id: None,
            child_turn_id: None,
            agent_role: None,
            depth: 1,
            max_depth: 3,
            next_fire_at: Some(1_000),
            result_preview: None,
            error_preview: None,
            created_at: 100,
            updated_at: 100,
        }
    }

    #[test]
    fn duplicate_wait_guard_blocks_same_pending_state() {
        assert!(duplicate_wait_should_block(&[prior(false, 0)], 0, 3, 0));
    }

    #[test]
    fn duplicate_wait_guard_allows_terminal_progress() {
        assert!(!duplicate_wait_should_block(&[prior(false, 0)], 1, 2, 0));
    }

    #[test]
    fn duplicate_wait_guard_allows_review_required_progress() {
        assert!(!duplicate_wait_should_block(&[prior(false, 0)], 0, 0, 1));
        assert!(!duplicate_wait_should_block(
            &[prior_with_review(false, 0, 0)],
            0,
            1,
            1
        ));
    }

    #[test]
    fn duplicate_wait_guard_allows_one_timeout_retry_then_blocks() {
        assert!(!duplicate_wait_should_block(&[prior(true, 0)], 0, 3, 0));
        assert!(duplicate_wait_should_block(
            &[prior(true, 0), prior(true, 0)],
            0,
            3,
            0
        ));
    }

    #[test]
    fn task_wait_tool_input_defaults_to_review_aware_mode() {
        let input: TaskWaitToolInput = serde_json::from_value(json!({
            "taskIds": ["task_1234567890123456"]
        }))
        .expect("task_wait input should decode");

        let params = input.into_params().expect("input should convert");

        assert_eq!(params.mode, TaskWaitMode::AllTerminalOrReviewRequired);
    }

    #[test]
    fn task_wait_signature_from_arguments_uses_tool_default_mode() {
        let signature = TaskWaitSignature::from_arguments(&json!({
            "taskIds": ["task_1234567890123456"]
        }))
        .expect("signature should decode");

        assert_eq!(signature.mode, TaskWaitMode::AllTerminalOrReviewRequired);
    }

    #[test]
    fn scheduled_task_create_output_is_not_waitable_without_run() {
        let response = TaskCreateResponse {
            task: sample_task(TaskStatus::Scheduled),
            trigger: sample_cron_trigger(Some(1_000)),
            run: None,
            agent_spec: None,
        };

        let output = task_create_tool_output(&response, &sample_task_turn_item());

        assert_eq!(output["waitable"], JsonValue::Bool(false));
        assert_eq!(output["runId"], JsonValue::Null);
        assert_eq!(
            output["waitRecommendation"],
            JsonValue::String("do_not_call_task_wait_confirm_schedule".to_owned())
        );
        assert_eq!(output["nextFireAt"], JsonValue::from(1_000));
        assert_eq!(output["trigger"]["kind"], "cron");
        assert_eq!(output["trigger"]["cronExpr"], "0 7 * * *");
        assert!(
            output["validationHints"]
                .as_array()
                .expect("validation hints should be an array")
                .iter()
                .any(|hint| hint == "do_not_call_task_wait_when_waitable_false")
        );
    }

    #[test]
    fn task_trigger_detail_output_keeps_model_facing_shape() {
        let trigger = sample_cron_trigger(Some(1_000));

        let output = task_trigger_detail_output(&trigger);

        assert_eq!(output["kind"], "cron");
        assert_eq!(output["cronExpr"], "0 7 * * *");
        assert_eq!(output["timezone"], "Europe/Moscow");
        assert_eq!(output["triggerId"], "trigger_1234567890123");
        assert_eq!(output["nextFireAt"], JsonValue::from(1_000));
        assert!(
            output.get("spec").is_none(),
            "model-facing trigger output must not expose internal trigger.spec"
        );
    }

    #[test]
    fn task_update_tool_output_uses_model_facing_trigger_shape() {
        let response = TaskUpdateResponse {
            task: sample_task(TaskStatus::Scheduled),
            trigger: Some(sample_cron_trigger(Some(1_000))),
            agent_spec: None,
            changed_fields: vec!["trigger".to_owned()],
        };

        let output = task_update_tool_output(&response);

        assert_eq!(output["trigger"]["kind"], "cron");
        assert_eq!(output["trigger"]["cronExpr"], "0 7 * * *");
        assert!(
            output["trigger"].get("spec").is_none(),
            "task_update output should not teach the model to use trigger.spec"
        );
    }

    #[test]
    fn task_wait_guard_detects_future_scheduled_task_without_active_run() {
        let response = sample_task_response(TaskStatus::Scheduled, Vec::new(), Some(1_000));

        assert!(task_wait_target_is_non_waitable_scheduled(&response, 500));

        let snapshot = non_waitable_scheduled_task_output(&response, 500);
        assert_eq!(snapshot["waitable"], JsonValue::Bool(false));
        assert_eq!(
            snapshot["reason"],
            JsonValue::String("future_scheduled_task_without_active_run".to_owned())
        );
    }

    #[test]
    fn task_wait_guard_allows_scheduled_task_with_active_run() {
        let response = sample_task_response(
            TaskStatus::Scheduled,
            vec![sample_run(TaskRunStatus::Running)],
            Some(1_000),
        );

        assert!(!task_wait_target_is_non_waitable_scheduled(&response, 500));
    }

    fn task_tool_invocation(tool_name: &str, arguments: JsonValue) -> ToolInvocation {
        ToolInvocation {
            call_id: "call_test".to_owned(),
            tool_name: tool_name.to_owned(),
            source: pioneer_tools::ToolCallSource::Model,
            payload: ToolPayload::Function { arguments },
            workdir: PathBuf::new(),
            environment: Default::default(),
            attempt_id: 0,
            idempotency_key: None,
            recovery: ToolRecoveryMetadata::default(),
            permission_metadata: pioneer_tools::ToolPermissionMetadata::default(),
            cancellation: tokio_util::sync::CancellationToken::new(),
        }
    }

    #[test]
    fn task_list_owner_kind_thread_without_owner_id_targets_current_thread() {
        let owner_id = normalize_task_list_owner_id(
            Some(TaskOwnerKind::Thread),
            None,
            "thread_12345678901234",
        )
        .expect("current thread owner id should be filled");

        assert_eq!(owner_id.as_deref(), Some("thread_12345678901234"));
    }

    #[test]
    fn task_list_owner_kind_thread_preserves_explicit_owner_id() {
        let owner_id = normalize_task_list_owner_id(
            Some(TaskOwnerKind::Thread),
            Some("thread_explicit123456".to_owned()),
            "thread_current1234567",
        )
        .expect("explicit thread owner id should be preserved");

        assert_eq!(owner_id.as_deref(), Some("thread_explicit123456"));
    }

    #[test]
    fn task_list_non_thread_owner_without_owner_id_stays_unscoped() {
        let owner_id = normalize_task_list_owner_id(
            Some(TaskOwnerKind::Workspace),
            None,
            "thread_current1234567",
        )
        .expect("workspace owner without owner id should stay unscoped");

        assert_eq!(owner_id, None);
    }

    #[test]
    fn task_list_owner_id_without_owner_kind_is_rejected() {
        let error = normalize_task_list_owner_id(
            None,
            Some("thread_current1234567".to_owned()),
            "thread_current1234567",
        )
        .expect_err("ownerId without ownerKind would be silently ignored by list_tasks");

        assert!(error.to_string().contains("`ownerId` requires `ownerKind`"));
    }

    #[test]
    fn mutation_idempotency_key_falls_back_to_tool_and_item_id() {
        let key = mutation_idempotency_key_for_item(TASK_CREATE_TOOL, "call_123", &json!({}))
            .expect("mutation key should always be derivable for completed mutation items");

        assert_eq!(key, "task_create:call_123");
    }

    #[test]
    fn task_create_schema_exposes_model_facing_trigger_shape() {
        let schema = task_create_schema();
        let schema_text = schema.to_string();
        assert!(
            schema_text.contains("cronExpr"),
            "task_create schema should expose the model-facing cronExpr field"
        );
        assert!(
            !schema_text.contains("TaskTriggerInput"),
            "task_create schema must not expose the internal TaskTriggerInput wrapper"
        );
        assert!(
            schema_text.contains("executorKind") && schema_text.contains("agent"),
            "task_create schema should document the only model-facing executor kind"
        );
        assert!(
            !schema_text.contains("\"trigger\":{\"type\":\"string\""),
            "task_create trigger must not be described as a string: {schema_text}"
        );
        assert!(
            schema
                .pointer("/properties/trigger")
                .is_some_and(|trigger| trigger.is_object()),
            "task_create trigger should be an object-valued schema property"
        );
    }

    #[test]
    fn task_tool_schemas_include_model_guidance_hints() {
        let create_schema = task_create_schema().to_string();
        for expected in [
            "Do not pass runtime-owned fields",
            "self-contained for future runs",
            "currently available tools/skills/MCP/built-ins",
            "Final result format and delivery contract",
            "do not wrap it in spec",
            "five-field cron expression",
            "Example value: 0 7 * * *",
            "Example value: Europe/Moscow",
            "Prefer inputText over input",
            "only. Omit this field unless explicitly setting",
        ] {
            assert!(
                create_schema.contains(expected),
                "task_create schema should include guidance `{expected}`, got: {create_schema}"
            );
        }

        let wait_schema = task_wait_schema().to_string();
        for expected in [
            "Use taskIds, not taskId",
            "exactly 21 characters",
            "do not use a single taskId field",
            "waitable=false",
            "confirm their schedule",
        ] {
            assert!(
                wait_schema.contains(expected),
                "task_wait schema should include guidance `{expected}`, got: {wait_schema}"
            );
        }

        let accept_schema_value = task_accept_schema();
        let accept_candidate_schema = accept_schema_value
            .pointer("/properties/candidateId")
            .expect("task_accept candidateId schema should exist");
        assert_eq!(
            accept_candidate_schema
                .get("maxLength")
                .and_then(JsonValue::as_u64),
            Some(128),
            "task_accept candidateId must accept review candidate ids, not only 21-char entity ids"
        );
        let accept_schema = accept_schema_value.to_string();
        for expected in [
            "candidate returned by task_wait reviewRequired",
            "taskId",
            "runId",
            "candidateId",
            "Use the exact candidateId",
            "Optional short reason",
        ] {
            assert!(
                accept_schema.contains(expected),
                "task_accept schema should include guidance `{expected}`, got: {accept_schema}"
            );
        }

        let revise_schema_value = task_revise_schema();
        let revise_candidate_schema = revise_schema_value
            .pointer("/properties/candidateId")
            .expect("task_revise candidateId schema should exist");
        assert_eq!(
            revise_candidate_schema
                .get("maxLength")
                .and_then(JsonValue::as_u64),
            Some(128),
            "task_revise candidateId must accept review candidate ids, not only 21-char entity ids"
        );

        let reschedule_schema = task_reschedule_schema().to_string();
        assert!(
            reschedule_schema.contains("do not wrap it in spec"),
            "task_reschedule schema should steer the model away from trigger.spec, got: {reschedule_schema}"
        );

        let update_schema = task_update_schema().to_string();
        for expected in [
            "Patch only the fields",
            "clear* flags",
            "self-contained executor instructions",
            "do not wrap it in spec",
        ] {
            assert!(
                update_schema.contains(expected),
                "task_update schema should include guidance `{expected}`, got: {update_schema}"
            );
        }
    }

    #[test]
    fn task_tool_hints_do_not_embed_json_object_examples() {
        for configured in task_tool_specs() {
            assert_no_json_object_example(
                configured.spec.description.as_str(),
                &format!("{} tool description", configured.spec.name),
            );
            assert_schema_descriptions_have_no_json_object_examples(
                &configured.spec.parameters,
                &configured.spec.name,
            );
            assert_no_json_object_example(
                task_tool_argument_hint(configured.spec.name.as_str()),
                &format!("{} validation hint", configured.spec.name),
            );
        }
    }

    fn assert_schema_descriptions_have_no_json_object_examples(value: &JsonValue, label: &str) {
        match value {
            JsonValue::Object(object) => {
                if let Some(description) = object.get("description").and_then(JsonValue::as_str) {
                    assert_no_json_object_example(description, label);
                }
                for value in object.values() {
                    assert_schema_descriptions_have_no_json_object_examples(value, label);
                }
            }
            JsonValue::Array(values) => {
                for value in values {
                    assert_schema_descriptions_have_no_json_object_examples(value, label);
                }
            }
            _ => {}
        }
    }

    fn assert_no_json_object_example(value: &str, label: &str) {
        assert!(
            !value.contains('{') && !value.contains('}'),
            "{label} should not include JSON object examples: {value}"
        );
    }

    #[test]
    fn task_create_tool_input_maps_flat_cron_trigger_to_protocol_trigger() {
        let input: TaskCreateToolInput = serde_json::from_value(json!({
            "title": "Daily Moscow weather",
            "goal": "Send the daily forecast at 07:00 Moscow time",
            "trigger": {
                "kind": "cron",
                "cronExpr": "0 7 * * *",
                "timezone": "Europe/Moscow"
            }
        }))
        .expect("model-facing task_create input should decode");
        let trigger = input
            .trigger
            .expect("trigger should exist")
            .into_trigger_input()
            .expect("trigger should map to protocol");
        assert_eq!(
            trigger.spec,
            TaskTriggerSpec::Cron {
                cron_expr: "0 7 * * *".to_owned(),
                timezone: "Europe/Moscow".to_owned(),
                catch_up_policy: None,
            }
        );
    }

    #[test]
    fn task_create_decode_error_points_model_away_from_internal_trigger_spec() {
        let error = decode_tool_args::<TaskCreateToolInput>(task_tool_invocation(
            TASK_CREATE_TOOL,
            json!({
                "title": "Daily Moscow weather",
                "goal": "Send the daily forecast at 07:00 Moscow time",
                "trigger": {
                    "spec": {
                        "kind": "cron",
                        "cronExpr": "0 7 * * *",
                        "timezone": "Europe/Moscow"
                    }
                }
            }),
        ))
        .expect_err("internal trigger spec wrapper should be rejected");
        let message = error.to_string();
        assert!(
            message.contains("Do not use trigger.spec"),
            "decode error should include the model-facing trigger contract, got: {message}"
        );
    }

    #[test]
    fn task_create_decode_normalizes_stringified_trigger_object() {
        let input = decode_tool_args::<TaskCreateToolInput>(task_tool_invocation(
            TASK_CREATE_TOOL,
            json!({
                "title": "Daily Moscow weather",
                "goal": "Send the daily forecast at 07:00 Moscow time",
                "trigger": "{\"kind\":\"cron\",\"cronExpr\":\"0 7 * * *\",\"timezone\":\"Europe/Moscow\"}",
                "instructions": ["Use a currently available weather capability."],
                "outputInstructions": "Return the forecast or a clear failure."
            }),
        ))
        .expect("stringified trigger object should normalize before decode");
        let trigger = input
            .trigger
            .expect("trigger should decode")
            .into_trigger_input()
            .expect("trigger should map to protocol");
        assert!(matches!(trigger.spec, TaskTriggerSpec::Cron { .. }));
    }

    #[test]
    fn task_create_decode_rejects_plain_trigger_string_with_shape_hint() {
        let error = decode_tool_args::<TaskCreateToolInput>(task_tool_invocation(
            TASK_CREATE_TOOL,
            json!({
                "title": "Daily Moscow weather",
                "goal": "Send the daily forecast at 07:00 Moscow time",
                "trigger": "every day at 07:00 Moscow",
            }),
        ))
        .expect_err("plain trigger string should be rejected");
        let message = error.to_string();
        assert!(message.contains("$.trigger"), "{message}");
        assert!(message.contains("must be a JSON object"), "{message}");
        assert!(message.contains("trigger.kind to cron"), "{message}");
        assert!(
            message.contains("trigger.cronExpr to 0 7 * * *"),
            "{message}"
        );
        assert!(
            message.contains("trigger.timezone to Europe/Moscow"),
            "{message}"
        );
    }

    #[test]
    fn task_update_decode_error_points_model_away_from_internal_trigger_spec() {
        let error = decode_tool_args::<TaskUpdateToolInput>(task_tool_invocation(
            TASK_UPDATE_TOOL,
            json!({
                "taskId": "123456789012345678901",
                "trigger": {
                    "spec": {
                        "kind": "cron",
                        "cronExpr": "0 7 * * *",
                        "timezone": "Europe/Moscow"
                    }
                }
            }),
        ))
        .expect_err("internal trigger spec wrapper should be rejected");
        let message = error.to_string();
        assert!(
            message.contains("Do not use trigger.spec"),
            "decode error should include the model-facing update trigger contract, got: {message}"
        );
    }

    #[test]
    fn task_update_tool_input_requires_a_patch_field() {
        let input: TaskUpdateToolInput = serde_json::from_value(json!({
            "taskId": "123456789012345678901"
        }))
        .expect("task_update input should decode");
        assert!(!input.has_patch_fields());

        let input: TaskUpdateToolInput = serde_json::from_value(json!({
            "taskId": "123456789012345678901",
            "clearInput": true
        }))
        .expect("task_update clear patch should decode");
        assert!(input.has_patch_fields());
    }

    #[test]
    fn task_agent_input_merges_input_text_with_structured_input() {
        let merged = merge_task_agent_input(
            Some("city=Moscow".to_owned()),
            Some(TaskAgentInput {
                text: None,
                variables: vec![pioneer_protocol::TaskAgentInputVariable {
                    name: "units".to_owned(),
                    value: pioneer_protocol::TaskValue::String("metric".to_owned()),
                }],
                attachments: Vec::new(),
                references: Vec::new(),
            }),
        )
        .expect("inputText should fill missing input.text")
        .expect("merged input should exist");
        assert_eq!(merged.text.as_deref(), Some("city=Moscow"));
        assert_eq!(merged.variables.len(), 1);

        merge_task_agent_input(
            Some("city=Moscow".to_owned()),
            Some(TaskAgentInput {
                text: Some("city=Berlin".to_owned()),
                variables: Vec::new(),
                attachments: Vec::new(),
                references: Vec::new(),
            }),
        )
        .expect_err("conflicting inputText and input.text should be rejected");
    }

    #[test]
    fn task_wait_decode_error_points_to_task_ids_not_task_id() {
        let error = decode_tool_args::<TaskWaitToolInput>(task_tool_invocation(
            TASK_WAIT_TOOL,
            json!({
                "taskId": "123456789012345678901",
                "timeoutMs": 1000
            }),
        ))
        .expect_err("taskId should be rejected in favor of taskIds");
        let message = error.to_string();
        assert!(message.contains("Use taskIds or runIds"), "{message}");
        assert!(message.contains("not a single taskId"), "{message}");
    }

    #[test]
    fn scheduled_task_create_requires_existing_prompt_fields() {
        let instructions = normalize_task_instructions(Vec::new(), TaskTriggerKind::Cron)
            .expect_err("scheduled task should require instructions");
        assert!(instructions.to_string().contains("`instructions`"));

        let output = normalize_task_output_instructions(None, TaskTriggerKind::Cron)
            .expect_err("scheduled task should require output instructions");
        assert!(output.to_string().contains("`outputInstructions`"));
    }

    #[test]
    fn scheduled_task_create_compiles_runtime_capability_guidance() {
        let instructions = normalize_task_instructions(
            vec![
                "Use an available weather or forecast capability for the requested city."
                    .to_owned(),
                "If forecast data is unavailable, report a clear failure.".to_owned(),
            ],
            TaskTriggerKind::Cron,
        )
        .expect("scheduled task instructions should normalize");
        let rendered = instructions.join("\n");
        assert!(rendered.contains("durable task"));
        assert!(rendered.contains("currently available tools, skills, MCP servers"));
        assert!(rendered.contains("unavailable at run time"));
        assert!(rendered.contains("weather or forecast capability"));
    }

    #[test]
    fn immediate_task_create_can_omit_instructions_for_concise_default() {
        let instructions = normalize_task_instructions(Vec::new(), TaskTriggerKind::Immediate)
            .expect("immediate task should keep concise default");
        assert_eq!(instructions, vec!["Return a concise final result."]);
        let output = normalize_task_output_instructions(None, TaskTriggerKind::Immediate)
            .expect("immediate task should not require output instructions");
        assert_eq!(output, None);
    }

    #[test]
    fn task_wait_rejects_missing_targets_and_truncated_ids() {
        let empty: TaskWaitToolInput =
            serde_json::from_value(json!({})).expect("empty wait input should decode");
        let error = empty
            .into_params()
            .expect_err("wait requires at least one id");
        assert!(error.to_string().contains("taskIds"));

        let truncated: TaskWaitToolInput = serde_json::from_value(json!({
            "taskIds": ["tas"]
        }))
        .expect("truncated id wait input should decode");
        let error = truncated
            .into_params()
            .expect_err("truncated task id should be rejected");
        assert!(error.to_string().contains("exactly 21 characters"));
    }
}
