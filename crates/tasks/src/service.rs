use crate::TaskRuntimeResult;
use crate::event_bus::{TaskEventBus, TaskEventFilter, TaskEventWakeDelivery};
use crate::executor::{
    TaskExecutionContext, TaskExecutionHandle, TaskExecutor, TaskExecutorRegistry,
};
use crate::policy::{
    TaskCreateContext, TaskMutationContext, TaskWaitContext, default_delivery_policy,
    default_lifecycle_policy, default_retry_policy,
};
use crate::projector::TaskProjector;
use crate::reconciliation::TaskStartupReconciler;
use crate::scheduler::TaskScheduler;
use crate::trigger::TaskTriggerCalculator;
use anyhow::{anyhow, bail};
use pioneer_crud::{AppendedTaskEvent, CrudStore};
use pioneer_protocol::{
    Task, TaskAgendaParams, TaskAgendaResponse, TaskAgentPrompt, TaskAgentReviewPolicy,
    TaskAgentSpec, TaskAgentWriteMode, TaskAttachmentMode, TaskCancelParams, TaskCancelResponse,
    TaskCancelScope, TaskConcurrencyConflictPolicy, TaskCreateParams, TaskCreateResponse,
    TaskDeliveriesParams, TaskDeliveriesResponse, TaskDelivery, TaskDeliveryAttempt,
    TaskDeliveryAttemptStatus, TaskDeliveryMode, TaskDeliveryStatus, TaskDetachParams,
    TaskDetachResponse, TaskError, TaskErrorClass, TaskEventPayload, TaskEventsParams,
    TaskEventsResponse, TaskExecutorKind, TaskGetParams, TaskGetResponse, TaskLifecyclePolicy,
    TaskListParams, TaskListResponse, TaskOwnerKind, TaskParentTerminalAction, TaskPauseParams,
    TaskPauseResponse, TaskRescheduleParams, TaskRescheduleResponse, TaskResultCandidate,
    TaskResultCandidateStatus, TaskResultReviewDecision, TaskResultReviewEvent,
    TaskResultReviewEventKind, TaskResultReviewerKind, TaskResumeParams, TaskResumeResponse,
    TaskRun, TaskRunExecutionStatus, TaskRunStatus, TaskStatus, TaskTree, TaskTreeParams,
    TaskTreeResponse, TaskTrigger, TaskTriggerKind, TaskTriggerStatus, TaskUpdateParams,
    TaskUpdateResponse, TaskWaitItem, TaskWaitMode, TaskWaitNonWaitableItem,
    TaskWaitNonWaitableReason, TaskWaitParams, TaskWaitResponse, TaskWaitReviewAction,
    TaskWaitReviewItem, TaskWaitRevisionBlockedReason, TaskWriteLock, TaskWriteLockConflict,
    TaskWriteLockScopeKind, TaskWriteLockStatus, generate_id,
};
use std::collections::VecDeque;
use std::future::Future;
use std::path::{Component, Path};
use std::pin::Pin;
use std::sync::{Arc, Weak};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{Duration, MissedTickBehavior, interval, timeout};

const ID_LEN: usize = 21;
const DEFAULT_MAX_TASK_DEPTH: i64 = 3;
const MAX_ROOT_TASK_DEPTH_LIMIT: i64 = 10;
const WAIT_RESCAN_INTERVAL: Duration = Duration::from_millis(250);

type TaskServiceFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// Task mutations append and project large event payloads; keep those futures off caller stacks.
fn task_service_future<'a, F, T>(future: F) -> TaskServiceFuture<'a, T>
where
    F: Future<Output = T> + Send + 'a,
{
    Box::pin(future)
}

pub struct TaskRuntime {
    service: Arc<TaskService>,
    scheduler: Arc<TaskScheduler>,
    event_bus: Arc<TaskEventBus>,
    executors: Arc<TaskExecutorRegistry>,
    reconciler: Arc<TaskStartupReconciler>,
    scheduler_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRuntimeConfig {
    pub review: TaskReviewRuntimeConfig,
}

impl Default for TaskRuntimeConfig {
    fn default() -> Self {
        Self {
            review: TaskReviewRuntimeConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskReviewRuntimeConfig {
    pub enabled: bool,
    pub allow_task_create_review_policy: bool,
    pub default_parent_review_for_immediate_attached_agent_tasks: bool,
    pub default_max_revision_rounds: u32,
}

impl Default for TaskReviewRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            allow_task_create_review_policy: false,
            default_parent_review_for_immediate_attached_agent_tasks: false,
            default_max_revision_rounds: 2,
        }
    }
}

impl TaskRuntime {
    pub fn new(store: Arc<CrudStore>) -> Self {
        Self::new_with_config(store, TaskRuntimeConfig::default())
    }

    pub fn new_with_config(store: Arc<CrudStore>, config: TaskRuntimeConfig) -> Self {
        let event_bus = Arc::new(TaskEventBus::new());
        let executors = Arc::new(TaskExecutorRegistry::new());
        let service = Arc::new(TaskService::new_with_config(
            store.clone(),
            event_bus.clone(),
            executors.clone(),
            config.clone(),
        ));
        let scheduler = Arc::new(TaskScheduler::new(
            store.clone(),
            event_bus.clone(),
            executors.clone(),
        ));
        service.set_scheduler(Arc::downgrade(&scheduler));
        let reconciler = Arc::new(TaskStartupReconciler::new(
            store,
            event_bus.clone(),
            executors.clone(),
            scheduler.handle(),
        ));
        Self {
            service,
            scheduler,
            event_bus,
            executors,
            reconciler,
            scheduler_task: Mutex::new(None),
        }
    }

    pub fn service(&self) -> Arc<TaskService> {
        self.service.clone()
    }

    pub fn event_bus(&self) -> Arc<TaskEventBus> {
        self.event_bus.clone()
    }

    pub async fn register_executor(&self, executor: Arc<dyn TaskExecutor>) {
        self.executors.register(executor).await;
    }

    pub async fn start(&self) -> TaskRuntimeResult<()> {
        let now = now_timestamp_secs();
        self.reconciler.reconcile(now).await?;
        self.service.recover_retry_and_lock_state(now).await?;
        self.service.recover_stuck_deliveries(now, 1024).await?;
        self.scheduler.process_due_once(now).await?;
        let mut guard = self.scheduler_task.lock().await;
        if guard.is_none() {
            let scheduler = self.scheduler.clone();
            *guard = Some(tokio::spawn(async move {
                scheduler.run().await;
            }));
        }
        Ok(())
    }

    pub async fn process_due_once(&self, now: i64) -> TaskRuntimeResult<usize> {
        self.scheduler.process_due_once(now).await
    }
}

pub struct TaskService {
    store: Arc<CrudStore>,
    projector: TaskProjector,
    event_bus: Arc<TaskEventBus>,
    executors: Arc<TaskExecutorRegistry>,
    scheduler: RwLock<Option<Weak<TaskScheduler>>>,
    config: TaskRuntimeConfig,
}

impl TaskService {
    pub fn new(
        store: Arc<CrudStore>,
        event_bus: Arc<TaskEventBus>,
        executors: Arc<TaskExecutorRegistry>,
    ) -> Self {
        Self::new_with_config(store, event_bus, executors, TaskRuntimeConfig::default())
    }

    pub fn new_with_config(
        store: Arc<CrudStore>,
        event_bus: Arc<TaskEventBus>,
        executors: Arc<TaskExecutorRegistry>,
        config: TaskRuntimeConfig,
    ) -> Self {
        let projector = TaskProjector::new(store.clone());
        Self {
            store,
            projector,
            event_bus,
            executors,
            scheduler: RwLock::new(None),
            config,
        }
    }

    pub fn set_scheduler(&self, scheduler: Weak<TaskScheduler>) {
        if let Ok(mut guard) = self.scheduler.try_write() {
            *guard = Some(scheduler);
        }
    }

    #[cfg(test)]
    pub(crate) fn store(&self) -> Arc<CrudStore> {
        self.store.clone()
    }

    pub async fn create_task(
        &self,
        _context: TaskCreateContext,
        params: TaskCreateParams,
    ) -> TaskRuntimeResult<TaskCreateResponse> {
        validate_create_params(&params)?;
        self.validate_review_policy_create_gate(&params)?;
        let now = now_timestamp_secs();
        let parent = self
            .parent_context(params.parent_task_id.as_deref())
            .await?;
        let trigger_kind = params.trigger.spec.kind();
        TaskTriggerCalculator::validate(&params.trigger.spec)?;

        let depth = parent.depth;
        let max_depth = normalize_max_depth(depth, parent.max_depth, params.agent_spec.as_ref())?;
        if depth > max_depth {
            bail!("task depth {depth} exceeds max depth {max_depth}");
        }

        let task_id = generate_id(ID_LEN);
        let trigger_id = generate_id(ID_LEN);
        let next_fire_at = TaskTriggerCalculator::initial_next_fire_at(&params.trigger.spec, now)?;
        let lifecycle_policy = params.lifecycle_policy.clone().unwrap_or_else(|| {
            default_lifecycle_policy(trigger_kind, params.created_by_turn_id.is_some())
        });
        let agent_review_policy =
            self.effective_agent_review_policy_for_create(&params, trigger_kind, &lifecycle_policy);
        let task = Task {
            id: task_id.clone(),
            workspace_id: params.workspace_id.clone(),
            owner_kind: params.owner_kind,
            owner_id: params.owner_id.clone(),
            created_by_thread_id: params.created_by_thread_id.clone(),
            created_by_turn_id: params.created_by_turn_id.clone(),
            root_task_id: parent.root_task_id.clone(),
            parent_task_id: params.parent_task_id.clone(),
            executor_kind: params.executor_kind,
            status: TaskStatus::Draft,
            title: required_trimmed(&params.title, "title")?,
            goal: required_trimmed(&params.goal, "goal")?,
            priority: params.priority,
            lifecycle_policy: Some(lifecycle_policy),
            delivery_policy: Some(params.delivery_policy.clone().unwrap_or_else(|| {
                default_delivery_policy(
                    trigger_kind,
                    params.owner_kind,
                    params.owner_id.as_deref(),
                    params.created_by_thread_id.as_deref(),
                )
            })),
            retry_policy: Some(
                params
                    .retry_policy
                    .clone()
                    .unwrap_or_else(default_retry_policy),
            ),
            timeout_policy: params.timeout_policy.clone(),
            concurrency_policy: params.concurrency_policy.clone(),
            metadata: params.metadata.clone(),
            result: None,
            error: None,
            revision: 1,
            created_at: now,
            updated_at: now,
            completed_at: None,
        };
        let trigger = TaskTrigger {
            id: trigger_id.clone(),
            task_id: task_id.clone(),
            status: TaskTriggerStatus::Active,
            spec: params.trigger.spec.clone(),
            next_fire_at,
            last_fire_at: None,
            created_at: now,
            updated_at: now,
        };
        let immediate_run = (trigger_kind == TaskTriggerKind::Immediate).then(|| TaskRun {
            id: generate_id(ID_LEN),
            task_id: task_id.clone(),
            trigger_id: Some(trigger_id.clone()),
            parent_run_id: None,
            run_group_id: generate_id(ID_LEN),
            attempt_number: 1,
            retry_of_run_id: None,
            ready_at: Some(now),
            run_number: 1,
            status: TaskRunStatus::Queued,
            executor_kind: params.executor_kind,
            started_at: None,
            completed_at: None,
            heartbeat_at: None,
            locked_by: None,
            lock_expires_at: None,
            result: None,
            error: None,
            created_at: now,
            updated_at: now,
        });
        let agent_spec = params.agent_spec.clone().map(|input| TaskAgentSpec {
            id: generate_id(ID_LEN),
            task_id: task_id.clone(),
            run_id: None,
            agent_role: input.agent_role,
            agent_nickname: input.agent_nickname,
            model: input.model,
            model_provider: input.model_provider,
            prompt: input.prompt,
            context_policy: input.context_policy,
            tool_policy: input.tool_policy,
            result_contract: input.result_contract,
            review_policy: agent_review_policy,
            depth,
            max_depth,
            created_at: now,
            updated_at: now,
        });

        let mut events = vec![
            TaskEventPayload::TaskCreated { task: task.clone() },
            TaskEventPayload::TriggerCreated {
                trigger: trigger.clone(),
            },
        ];
        if let Some(agent_spec) = agent_spec.clone() {
            events.push(TaskEventPayload::AgentSpecCreated { agent_spec });
        }
        match trigger_kind {
            TaskTriggerKind::Immediate => events.push(TaskEventPayload::TaskQueued {
                task_id: task_id.clone(),
                run_id: immediate_run.as_ref().map(|run| run.id.clone()),
            }),
            TaskTriggerKind::ScheduledAt | TaskTriggerKind::Interval | TaskTriggerKind::Cron => {
                events.push(TaskEventPayload::TaskScheduled {
                    task_id: task_id.clone(),
                    trigger_id: trigger_id.clone(),
                    next_fire_at,
                })
            }
            _ => {}
        }
        if let Some(run) = immediate_run.clone() {
            events.push(TaskEventPayload::RunCreated {
                run: run.clone(),
                agent_spec: agent_spec.clone().map(|mut spec| {
                    spec.run_id = Some(run.id.clone());
                    spec.updated_at = now;
                    spec
                }),
            });
            let mut exhausted_trigger = trigger.clone();
            exhausted_trigger.last_fire_at = Some(now);
            exhausted_trigger.next_fire_at = None;
            exhausted_trigger.status = TaskTriggerStatus::Exhausted;
            exhausted_trigger.updated_at = now;
            events.push(TaskEventPayload::TaskRescheduled {
                task_id: task_id.clone(),
                trigger: exhausted_trigger,
                rescheduled_at: now,
                reason: pioneer_protocol::TaskRescheduleReason::TriggerFired,
            });
        }
        let appended = self.append_events(events, now).await?;
        self.publish_and_wake(appended).await;
        if trigger_kind == TaskTriggerKind::Immediate {
            self.process_due_once(now).await?;
        }

        let response = self
            .store
            .get_task(task_id.as_str())
            .await?
            .ok_or_else(|| anyhow!("task disappeared after creation"))?;
        Ok(TaskCreateResponse {
            task: response.task,
            trigger: response
                .triggers
                .into_iter()
                .find(|candidate| candidate.id == trigger_id)
                .unwrap_or(trigger),
            run: response.runs.last().cloned(),
            agent_spec: response
                .agent_specs
                .into_iter()
                .find(|spec| spec.run_id.is_none())
                .or(agent_spec),
        })
    }

    pub async fn wait_tasks(
        &self,
        _context: TaskWaitContext,
        mut params: TaskWaitParams,
    ) -> TaskRuntimeResult<TaskWaitResponse> {
        if params.task_ids.is_empty() && params.run_ids.is_empty() {
            bail!("`task_ids` or `run_ids` is required");
        }
        if !params.return_completed && !params.return_pending {
            params.return_completed = true;
            params.return_pending = true;
        }

        let plan = self.build_wait_target_plan(&params).await?;
        let initial = self.collect_wait_state_for_plan(&plan).await?;
        if wait_condition_satisfied(&initial) {
            return Ok(initial);
        }
        if !has_wait_targets(&plan.wait_params) {
            return Ok(initial);
        }

        let mut subscription = self.event_bus.subscribe(TaskEventFilter {
            task_ids: plan.wait_params.task_ids.clone(),
            run_ids: plan.wait_params.run_ids.clone(),
            ..Default::default()
        });
        let after_subscribe = self.collect_wait_state_for_plan(&plan).await?;
        if wait_condition_satisfied(&after_subscribe) {
            return Ok(after_subscribe);
        }

        let wait_future = async {
            let mut bus_closed = false;
            let mut rescan = interval(WAIT_RESCAN_INTERVAL);
            rescan.set_missed_tick_behavior(MissedTickBehavior::Delay);
            rescan.tick().await;
            loop {
                if bus_closed {
                    rescan.tick().await;
                } else {
                    tokio::select! {
                        delivery = subscription.recv() => {
                            match delivery {
                                TaskEventWakeDelivery::Wake(_) | TaskEventWakeDelivery::Lagged(_) => {}
                                TaskEventWakeDelivery::Closed => bus_closed = true,
                            }
                        }
                        _ = rescan.tick() => {}
                    };
                }

                let response = self.collect_wait_state_for_plan(&plan).await?;
                if wait_condition_satisfied(&response) {
                    return Ok(response);
                }
            }
        };

        if let Some(timeout_ms) = params.timeout_ms {
            match timeout(Duration::from_millis(timeout_ms), wait_future).await {
                Ok(response) => response,
                Err(_) => {
                    let mut response = self.collect_wait_state_for_plan(&plan).await?;
                    response.timed_out = true;
                    Ok(response)
                }
            }
        } else {
            wait_future.await
        }
    }

    pub async fn get_wait_state_snapshot(
        &self,
        mut params: TaskWaitParams,
    ) -> TaskRuntimeResult<TaskWaitResponse> {
        if params.task_ids.is_empty() && params.run_ids.is_empty() {
            bail!("`task_ids` or `run_ids` is required");
        }
        if !params.return_completed && !params.return_pending {
            params.return_completed = true;
            params.return_pending = true;
        }
        let plan = self.build_wait_target_plan(&params).await?;
        self.collect_wait_state_for_plan(&plan).await
    }

    pub async fn cancel_task(
        &self,
        _context: TaskMutationContext,
        params: TaskCancelParams,
    ) -> TaskRuntimeResult<TaskCancelResponse> {
        let Some(root_response) = self.store.get_task(params.task_id.as_str()).await? else {
            bail!("task `{}` not found", params.task_id);
        };
        if is_terminal_task(root_response.task.status) {
            return Ok(TaskCancelResponse {
                task: root_response.task,
                cancelled_tasks: Vec::new(),
                detached_tasks: Vec::new(),
                kept_tasks: Vec::new(),
                cancelled_runs: Vec::new(),
                cancelled_deliveries: Vec::new(),
            });
        }

        let now = now_timestamp_secs();
        let reason = params
            .reason
            .clone()
            .unwrap_or_else(|| "task cancelled".to_owned());
        let tree = self
            .store
            .get_task_tree(params.task_id.as_str())
            .await?
            .ok_or_else(|| anyhow!("task `{}` disappeared during cancellation", params.task_id))?;
        let plan = plan_cancellation(&tree, params.scope);
        let mut events = Vec::new();
        let mut cancelled_runs = Vec::new();
        let mut cancelled_deliveries = Vec::new();
        let mut cancelled_executions = Vec::new();

        for task in &plan.cancelled_tasks {
            let Some(response) = self.store.get_task(task.id.as_str()).await? else {
                continue;
            };
            self.push_cancel_task_events(
                &response,
                reason.as_str(),
                now,
                &mut events,
                &mut cancelled_runs,
                &mut cancelled_deliveries,
                &mut cancelled_executions,
            )
            .await?;
        }

        for task in &plan.detached_tasks {
            let Some(response) = self.store.get_task(task.id.as_str()).await? else {
                continue;
            };
            if is_terminal_task(response.task.status) {
                continue;
            }
            let mut detached = response.task;
            let mut lifecycle = detached
                .lifecycle_policy
                .clone()
                .unwrap_or_else(|| default_lifecycle_policy(TaskTriggerKind::Immediate, false));
            lifecycle.attachment = TaskAttachmentMode::Detached;
            lifecycle.on_parent_cancel = TaskParentTerminalAction::KeepRunning;
            detached.lifecycle_policy = Some(lifecycle);
            detached.updated_at = now;
            detached.revision = detached.revision.saturating_add(1);
            events.push(TaskEventPayload::TaskDetached {
                task: detached,
                detached_at: now,
            });
        }

        let appended = self.append_events(events, now).await?;
        for (execution_id, error) in cancelled_executions {
            let _ = self
                .store
                .mark_execution_terminal(
                    execution_id.as_str(),
                    TaskRunExecutionStatus::Cancelled,
                    now,
                    None,
                    error.as_ref(),
                )
                .await?;
        }
        self.publish_and_wake(appended).await;
        let task = self
            .store
            .get_task(params.task_id.as_str())
            .await?
            .ok_or_else(|| anyhow!("task disappeared after cancellation"))?
            .task;
        Ok(TaskCancelResponse {
            task,
            cancelled_tasks: plan.cancelled_tasks,
            detached_tasks: plan.detached_tasks,
            kept_tasks: plan.kept_tasks,
            cancelled_runs,
            cancelled_deliveries,
        })
    }

    pub async fn detach_task(
        &self,
        _context: TaskMutationContext,
        params: TaskDetachParams,
    ) -> TaskRuntimeResult<TaskDetachResponse> {
        let Some(response) = self.store.get_task(params.task_id.as_str()).await? else {
            bail!("task `{}` not found", params.task_id);
        };
        let mut task = response.task;
        if is_terminal_task(task.status) {
            return Ok(TaskDetachResponse { task });
        }
        if task.status == TaskStatus::WaitingReview
            || response
                .runs
                .iter()
                .any(|run| run.status == TaskRunStatus::WaitingReview)
        {
            bail!(
                "task `{}` is waiting for review; accept, revise, or cancel the active review candidate before detaching",
                task.id
            );
        }
        let mut lifecycle = task.lifecycle_policy.clone().unwrap_or_else(|| {
            default_lifecycle_policy(
                response
                    .triggers
                    .first()
                    .map(|trigger| trigger.kind())
                    .unwrap_or(TaskTriggerKind::Immediate),
                task.created_by_turn_id.is_some(),
            )
        });
        lifecycle.attachment = pioneer_protocol::TaskAttachmentMode::Detached;
        task.lifecycle_policy = Some(lifecycle);
        task.revision = task.revision.saturating_add(1);
        task.updated_at = now_timestamp_secs();
        let appended = self
            .append_event(
                TaskEventPayload::TaskDetached {
                    task: task.clone(),
                    detached_at: task.updated_at,
                },
                task.updated_at,
            )
            .await?;
        self.publish_and_wake(vec![appended]).await;
        Ok(TaskDetachResponse { task })
    }

    pub async fn update_task(
        &self,
        _context: TaskMutationContext,
        params: TaskUpdateParams,
    ) -> TaskRuntimeResult<TaskUpdateResponse> {
        validate_update_params(&params)?;
        let Some(response) = self.store.get_task(params.task_id.as_str()).await? else {
            bail!("task `{}` not found", params.task_id);
        };
        if is_terminal_task(response.task.status) {
            bail!("terminal task `{}` cannot be updated", params.task_id);
        }
        if let Some(expected_revision) = params.expected_revision
            && response.task.revision != expected_revision
        {
            bail!(
                "task `{}` revision mismatch: expected {}, got {}",
                params.task_id,
                expected_revision,
                response.task.revision
            );
        }

        let now = now_timestamp_secs();
        let wants_agent_update = update_has_agent_patch(&params);
        let mut task = response.task.clone();
        let mut changed_fields = Vec::new();

        if let Some(title) = params.title {
            let title = required_trimmed(&title, "title")?;
            set_changed(&mut task.title, title, "title", &mut changed_fields);
        }
        if let Some(goal) = params.goal {
            let goal = required_trimmed(&goal, "goal")?;
            set_changed(&mut task.goal, goal, "goal", &mut changed_fields);
        }
        if let Some(priority) = params.priority {
            set_changed(
                &mut task.priority,
                priority,
                "priority",
                &mut changed_fields,
            );
        }
        if let Some(policy) = params.lifecycle_policy {
            set_option_changed(
                &mut task.lifecycle_policy,
                Some(policy),
                "lifecyclePolicy",
                &mut changed_fields,
            );
        }
        if let Some(policy) = params.delivery_policy {
            validate_delivery_policy(&policy)?;
            set_option_changed(
                &mut task.delivery_policy,
                Some(policy),
                "deliveryPolicy",
                &mut changed_fields,
            );
        }
        if let Some(policy) = params.retry_policy {
            set_option_changed(
                &mut task.retry_policy,
                Some(policy),
                "retryPolicy",
                &mut changed_fields,
            );
        }
        if params.clear_timeout_policy {
            set_option_changed(
                &mut task.timeout_policy,
                None,
                "timeoutPolicy",
                &mut changed_fields,
            );
        } else if let Some(policy) = params.timeout_policy {
            set_option_changed(
                &mut task.timeout_policy,
                Some(policy),
                "timeoutPolicy",
                &mut changed_fields,
            );
        }
        if params.clear_concurrency_policy {
            set_option_changed(
                &mut task.concurrency_policy,
                None,
                "concurrencyPolicy",
                &mut changed_fields,
            );
        } else if let Some(policy) = params.concurrency_policy {
            set_option_changed(
                &mut task.concurrency_policy,
                Some(policy),
                "concurrencyPolicy",
                &mut changed_fields,
            );
        }
        if params.clear_metadata {
            set_option_changed(&mut task.metadata, None, "metadata", &mut changed_fields);
        } else if let Some(metadata) = params.metadata {
            set_option_changed(
                &mut task.metadata,
                Some(metadata),
                "metadata",
                &mut changed_fields,
            );
        }

        let current_trigger = response.triggers.last().cloned();
        let mut updated_trigger = None;
        if let Some(trigger_input) = params.trigger {
            TaskTriggerCalculator::validate(&trigger_input.spec)?;
            let mut trigger = current_trigger.clone().unwrap_or_else(|| TaskTrigger {
                id: generate_id(ID_LEN),
                task_id: task.id.clone(),
                status: TaskTriggerStatus::Active,
                spec: trigger_input.spec.clone(),
                next_fire_at: None,
                last_fire_at: None,
                created_at: now,
                updated_at: now,
            });
            let next_fire_at =
                TaskTriggerCalculator::initial_next_fire_at(&trigger_input.spec, now)?;
            let changed = trigger.spec != trigger_input.spec
                || trigger.status != TaskTriggerStatus::Active
                || trigger.next_fire_at != next_fire_at;
            if changed {
                trigger.spec = trigger_input.spec;
                trigger.status = TaskTriggerStatus::Active;
                trigger.next_fire_at = next_fire_at;
                trigger.updated_at = now;
                push_changed(&mut changed_fields, "trigger");
                updated_trigger = Some(trigger);
            }
        }

        let final_trigger_kind = updated_trigger
            .as_ref()
            .or(current_trigger.as_ref())
            .map(TaskTrigger::kind)
            .unwrap_or(TaskTriggerKind::Immediate);
        let mut updated_agent_spec = None;
        let task_goal_changed = changed_fields.iter().any(|field| field == "goal");
        let existing_base_agent_spec = response
            .agent_specs
            .iter()
            .rev()
            .find(|spec| spec.run_id.is_none())
            .cloned();
        let materialized_base_agent_spec = existing_base_agent_spec.is_none()
            && matches!(
                final_trigger_kind,
                TaskTriggerKind::ScheduledAt | TaskTriggerKind::Interval | TaskTriggerKind::Cron
            );
        let base_agent_spec = existing_base_agent_spec.or_else(|| {
            response
                .agent_specs
                .iter()
                .rev()
                .next()
                .cloned()
                .map(|mut spec| {
                    spec.id = generate_id(ID_LEN);
                    spec.run_id = None;
                    spec.created_at = now;
                    spec.updated_at = now;
                    spec
                })
        });

        if task.executor_kind != TaskExecutorKind::Agent && wants_agent_update {
            bail!("non-agent task `{}` cannot update agent fields", task.id);
        }
        if task.executor_kind == TaskExecutorKind::Agent {
            if wants_agent_update || task_goal_changed || materialized_base_agent_spec {
                let original_spec = base_agent_spec
                    .clone()
                    .ok_or_else(|| anyhow!("agent task `{}` has no base agent spec", task.id))?;
                let mut spec = original_spec.clone();
                if task_goal_changed {
                    set_changed(
                        &mut spec.prompt.goal,
                        task.goal.clone(),
                        "goal",
                        &mut changed_fields,
                    );
                }
                if params.clear_agent_role {
                    set_option_changed(
                        &mut spec.agent_role,
                        None,
                        "agentRole",
                        &mut changed_fields,
                    );
                } else if let Some(agent_role) = params.agent_role {
                    set_option_changed(
                        &mut spec.agent_role,
                        clean_required_optional(agent_role, "agentRole")?,
                        "agentRole",
                        &mut changed_fields,
                    );
                }
                if params.clear_agent_nickname {
                    set_option_changed(
                        &mut spec.agent_nickname,
                        None,
                        "agentNickname",
                        &mut changed_fields,
                    );
                } else if let Some(agent_nickname) = params.agent_nickname {
                    set_option_changed(
                        &mut spec.agent_nickname,
                        clean_required_optional(agent_nickname, "agentNickname")?,
                        "agentNickname",
                        &mut changed_fields,
                    );
                }
                if let Some(instructions) = params.instructions {
                    set_changed(
                        &mut spec.prompt.instructions,
                        normalize_agent_instructions(instructions),
                        "instructions",
                        &mut changed_fields,
                    );
                }
                let merged_input =
                    merge_agent_input(params.input_text.clone(), params.input.clone())?;
                if params.clear_input {
                    set_option_changed(&mut spec.prompt.input, None, "input", &mut changed_fields);
                } else if let Some(input) = merged_input {
                    set_option_changed(
                        &mut spec.prompt.input,
                        Some(input),
                        "input",
                        &mut changed_fields,
                    );
                }
                if params.clear_output_instructions {
                    set_option_changed(
                        &mut spec.prompt.output_instructions,
                        None,
                        "outputInstructions",
                        &mut changed_fields,
                    );
                } else if let Some(output_instructions) = params.output_instructions {
                    set_option_changed(
                        &mut spec.prompt.output_instructions,
                        clean_required_optional(output_instructions, "outputInstructions")?,
                        "outputInstructions",
                        &mut changed_fields,
                    );
                }
                if params.clear_context_policy {
                    set_option_changed(
                        &mut spec.context_policy,
                        None,
                        "contextPolicy",
                        &mut changed_fields,
                    );
                } else if let Some(policy) = params.context_policy {
                    set_option_changed(
                        &mut spec.context_policy,
                        Some(policy),
                        "contextPolicy",
                        &mut changed_fields,
                    );
                }
                if params.clear_tool_policy {
                    set_option_changed(
                        &mut spec.tool_policy,
                        None,
                        "toolPolicy",
                        &mut changed_fields,
                    );
                } else if let Some(policy) = params.tool_policy {
                    set_option_changed(
                        &mut spec.tool_policy,
                        Some(policy),
                        "toolPolicy",
                        &mut changed_fields,
                    );
                }
                if params.clear_result_contract {
                    set_option_changed(
                        &mut spec.result_contract,
                        None,
                        "resultContract",
                        &mut changed_fields,
                    );
                } else if let Some(contract) = params.result_contract {
                    set_option_changed(
                        &mut spec.result_contract,
                        Some(contract),
                        "resultContract",
                        &mut changed_fields,
                    );
                }
                validate_agent_prompt_for_trigger(final_trigger_kind, &spec.prompt)?;
                if materialized_base_agent_spec {
                    push_changed(&mut changed_fields, "agentSpec");
                }
                if materialized_base_agent_spec || spec != original_spec {
                    spec.updated_at = now;
                    updated_agent_spec = Some(spec);
                }
            } else if let Some(agent_spec) = base_agent_spec.as_ref() {
                validate_agent_prompt_for_trigger(final_trigger_kind, &agent_spec.prompt)?;
            } else if matches!(
                final_trigger_kind,
                TaskTriggerKind::ScheduledAt | TaskTriggerKind::Interval | TaskTriggerKind::Cron
            ) {
                bail!("scheduled agent task `{}` has no base agent spec", task.id);
            }
        }

        if changed_fields.is_empty() {
            return Ok(TaskUpdateResponse {
                task,
                trigger: None,
                agent_spec: None,
                changed_fields,
            });
        }

        task.updated_at = now;
        task.revision = task.revision.saturating_add(1);
        let appended = self
            .append_event(
                TaskEventPayload::TaskUpdated {
                    task: task.clone(),
                    trigger: updated_trigger.clone(),
                    agent_spec: updated_agent_spec.clone(),
                    changed_fields: changed_fields.clone(),
                    updated_at: now,
                },
                now,
            )
            .await?;
        self.publish_and_wake(vec![appended]).await;
        if updated_trigger.is_some() {
            self.process_due_once(now).await?;
        }
        let response = self
            .store
            .get_task(task.id.as_str())
            .await?
            .ok_or_else(|| anyhow!("task disappeared after update"))?;
        Ok(TaskUpdateResponse {
            task: response.task,
            trigger: updated_trigger,
            agent_spec: updated_agent_spec,
            changed_fields,
        })
    }

    pub async fn reschedule_task(
        &self,
        _context: TaskMutationContext,
        params: TaskRescheduleParams,
    ) -> TaskRuntimeResult<TaskRescheduleResponse> {
        let Some(response) = self.store.get_task(params.task_id.as_str()).await? else {
            bail!("task `{}` not found", params.task_id);
        };
        if is_terminal_task(response.task.status) {
            bail!("terminal task `{}` cannot be rescheduled", params.task_id);
        }
        TaskTriggerCalculator::validate(&params.trigger.spec)?;
        let now = now_timestamp_secs();
        let trigger_id = response
            .triggers
            .last()
            .map(|trigger| trigger.id.clone())
            .unwrap_or_else(|| generate_id(ID_LEN));
        let next_fire_at = TaskTriggerCalculator::initial_next_fire_at(&params.trigger.spec, now)?;
        let trigger = TaskTrigger {
            id: trigger_id,
            task_id: params.task_id.clone(),
            status: TaskTriggerStatus::Active,
            spec: params.trigger.spec,
            next_fire_at,
            last_fire_at: response
                .triggers
                .last()
                .and_then(|trigger| trigger.last_fire_at),
            created_at: response
                .triggers
                .last()
                .map(|trigger| trigger.created_at)
                .unwrap_or(now),
            updated_at: now,
        };
        let appended = self
            .append_event(
                TaskEventPayload::TaskRescheduled {
                    task_id: params.task_id.clone(),
                    trigger: trigger.clone(),
                    rescheduled_at: now,
                    reason: pioneer_protocol::TaskRescheduleReason::UserRequested,
                },
                now,
            )
            .await?;
        self.publish_and_wake(vec![appended]).await;
        self.process_due_once(now).await?;
        let task = self
            .store
            .get_task(params.task_id.as_str())
            .await?
            .ok_or_else(|| anyhow!("task disappeared after reschedule"))?
            .task;
        Ok(TaskRescheduleResponse { task, trigger })
    }

    pub async fn pause_task(
        &self,
        _context: TaskMutationContext,
        params: TaskPauseParams,
    ) -> TaskRuntimeResult<TaskPauseResponse> {
        let Some(response) = self.store.get_task(params.task_id.as_str()).await? else {
            bail!("task `{}` not found", params.task_id);
        };
        if is_terminal_task(response.task.status) {
            return Ok(TaskPauseResponse {
                task: response.task,
                triggers: response.triggers,
            });
        }
        let now = now_timestamp_secs();
        let mut task = response.task;
        task.updated_at = now;
        task.revision = task.revision.saturating_add(1);
        let triggers = response
            .triggers
            .into_iter()
            .map(|mut trigger| {
                if trigger.status == TaskTriggerStatus::Active {
                    trigger.status = TaskTriggerStatus::Paused;
                    trigger.updated_at = now;
                }
                trigger
            })
            .collect::<Vec<_>>();
        let appended = task_service_future(self.append_event(
            TaskEventPayload::TaskPaused {
                task: task.clone(),
                triggers: triggers.clone(),
                reason: params.reason,
                paused_at: now,
            },
            now,
        ))
        .await?;
        self.publish_and_wake(vec![appended]).await;
        Ok(TaskPauseResponse { task, triggers })
    }

    pub async fn resume_task(
        &self,
        _context: TaskMutationContext,
        params: TaskResumeParams,
    ) -> TaskRuntimeResult<TaskResumeResponse> {
        let Some(response) = self.store.get_task(params.task_id.as_str()).await? else {
            bail!("task `{}` not found", params.task_id);
        };
        if is_terminal_task(response.task.status) {
            bail!("terminal task `{}` cannot be resumed", params.task_id);
        }
        let now = now_timestamp_secs();
        let mut task = response.task;
        let mut resumed_any = false;
        let mut triggers = Vec::with_capacity(response.triggers.len());
        for mut trigger in response.triggers {
            if trigger.status == TaskTriggerStatus::Paused {
                trigger.status = TaskTriggerStatus::Active;
                trigger.next_fire_at = resume_next_fire_at(&trigger, now)?;
                trigger.updated_at = now;
                resumed_any = true;
            }
            triggers.push(trigger);
        }
        if !resumed_any {
            return Ok(TaskResumeResponse { task, triggers });
        }
        task.status = if triggers
            .iter()
            .any(|trigger| trigger.next_fire_at.is_some())
        {
            TaskStatus::Scheduled
        } else {
            TaskStatus::Waiting
        };
        task.updated_at = now;
        task.revision = task.revision.saturating_add(1);
        let appended = task_service_future(self.append_event(
            TaskEventPayload::TaskResumed {
                task: task.clone(),
                triggers: triggers.clone(),
                reason: params.reason,
                resumed_at: now,
            },
            now,
        ))
        .await?;
        self.publish_and_wake(vec![appended]).await;
        self.process_due_once(now).await?;
        Ok(TaskResumeResponse { task, triggers })
    }

    pub async fn list_agenda(
        &self,
        params: TaskAgendaParams,
    ) -> TaskRuntimeResult<TaskAgendaResponse> {
        Ok(self.store.list_task_agenda(params).await?)
    }

    pub async fn list_deliveries(
        &self,
        params: TaskDeliveriesParams,
    ) -> TaskRuntimeResult<TaskDeliveriesResponse> {
        Ok(self.store.list_task_deliveries(params).await?)
    }

    pub async fn start_delivery(
        &self,
        delivery_id: &str,
        started_at: i64,
    ) -> TaskRuntimeResult<Option<(TaskDelivery, TaskDeliveryAttempt)>> {
        let Some(mut delivery) = self.store.get_task_delivery(delivery_id).await? else {
            return Ok(None);
        };
        if delivery.status != TaskDeliveryStatus::Pending {
            return Ok(None);
        }
        if let Some(next_attempt_at) = delivery.next_attempt_at
            && next_attempt_at > started_at
        {
            return Ok(None);
        }
        let attempt_number = delivery.attempt_count.saturating_add(1);
        delivery.status = TaskDeliveryStatus::Delivering;
        delivery.attempt_count = attempt_number;
        delivery.next_attempt_at = None;
        delivery.updated_at = started_at;
        let attempt = TaskDeliveryAttempt {
            id: generate_id(ID_LEN),
            delivery_id: delivery.id.clone(),
            attempt_number,
            status: TaskDeliveryAttemptStatus::Started,
            started_at,
            completed_at: None,
            http_status: None,
            error: None,
            response_fingerprint: None,
        };
        let appended = self
            .append_event(
                TaskEventPayload::DeliveryStarted {
                    delivery: delivery.clone(),
                    attempt: attempt.clone(),
                },
                started_at,
            )
            .await?;
        self.event_bus.publish(appended).await;
        Ok(Some((delivery, attempt)))
    }

    pub async fn complete_delivery(
        &self,
        mut delivery: TaskDelivery,
        mut attempt: TaskDeliveryAttempt,
        delivered_turn_id: Option<String>,
        delivered_notification_id: Option<String>,
        http_status: Option<u16>,
        response_fingerprint: Option<String>,
        delivered_at: i64,
    ) -> TaskRuntimeResult<TaskDelivery> {
        delivery.status = TaskDeliveryStatus::Delivered;
        delivery.delivered_turn_id = delivered_turn_id;
        delivery.delivered_notification_id = delivered_notification_id;
        delivery.delivered_at = Some(delivered_at);
        delivery.next_attempt_at = None;
        delivery.last_error = None;
        delivery.updated_at = delivered_at;
        attempt.status = TaskDeliveryAttemptStatus::Delivered;
        attempt.completed_at = Some(delivered_at);
        attempt.http_status = http_status;
        attempt.response_fingerprint = response_fingerprint;
        let appended = self
            .append_event(
                TaskEventPayload::DeliveryDelivered {
                    delivery: delivery.clone(),
                    attempt,
                },
                delivered_at,
            )
            .await?;
        self.event_bus.publish(appended).await;
        Ok(delivery)
    }

    pub async fn fail_delivery(
        &self,
        mut delivery: TaskDelivery,
        mut attempt: TaskDeliveryAttempt,
        error: String,
        http_status: Option<u16>,
        response_fingerprint: Option<String>,
        failed_at: i64,
    ) -> TaskRuntimeResult<TaskDelivery> {
        let retryable = delivery.attempt_count < delivery.max_attempts;
        delivery.status = if retryable {
            TaskDeliveryStatus::Pending
        } else {
            TaskDeliveryStatus::Failed
        };
        delivery.next_attempt_at = retryable.then_some(failed_at.saturating_add(60));
        delivery.last_error = Some(error.clone());
        delivery.updated_at = failed_at;
        attempt.status = TaskDeliveryAttemptStatus::Failed;
        attempt.completed_at = Some(failed_at);
        attempt.http_status = http_status;
        attempt.error = Some(error);
        attempt.response_fingerprint = response_fingerprint;
        let appended = self
            .append_event(
                TaskEventPayload::DeliveryFailed {
                    delivery: delivery.clone(),
                    attempt,
                },
                failed_at,
            )
            .await?;
        self.event_bus.publish(appended).await;
        Ok(delivery)
    }

    pub async fn recover_stuck_deliveries(&self, now: i64, limit: u64) -> TaskRuntimeResult<usize> {
        let cutoff = now.saturating_sub(300);
        let deliveries = self.store.list_stuck_task_deliveries(cutoff, limit).await?;
        let mut recovered = 0usize;
        for mut delivery in deliveries {
            delivery.status = TaskDeliveryStatus::Pending;
            delivery.next_attempt_at = Some(now);
            delivery.last_error = Some("delivery recovered after gateway restart".to_owned());
            delivery.updated_at = now;
            let appended = self
                .append_event(TaskEventPayload::DeliveryQueued { delivery }, now)
                .await?;
            self.event_bus.publish(appended).await;
            recovered = recovered.saturating_add(1);
        }
        Ok(recovered)
    }

    pub async fn get_task(&self, params: TaskGetParams) -> TaskRuntimeResult<TaskGetResponse> {
        self.store
            .get_task(params.task_id.as_str())
            .await?
            .ok_or_else(|| anyhow!("task `{}` not found", params.task_id))
    }

    pub async fn list_tasks(&self, params: TaskListParams) -> TaskRuntimeResult<TaskListResponse> {
        Ok(TaskListResponse {
            tasks: self.store.list_tasks(params).await?,
        })
    }

    pub async fn get_task_tree(
        &self,
        params: TaskTreeParams,
    ) -> TaskRuntimeResult<TaskTreeResponse> {
        self.store
            .get_task_tree(params.task_id.as_str())
            .await?
            .map(|tree| TaskTreeResponse { tree })
            .ok_or_else(|| anyhow!("task `{}` not found", params.task_id))
    }

    pub async fn get_task_events(
        &self,
        params: TaskEventsParams,
    ) -> TaskRuntimeResult<TaskEventsResponse> {
        self.store
            .get_task_events(params.task_id.as_str(), params.after_sequence)
            .await
    }

    pub async fn list_task_events_after(
        &self,
        task_id: &str,
        after_sequence: i64,
    ) -> TaskRuntimeResult<Vec<AppendedTaskEvent>> {
        self.store
            .list_task_events_after(task_id, after_sequence)
            .await
    }

    pub async fn list_task_event_task_ids(&self) -> TaskRuntimeResult<Vec<String>> {
        self.store.list_task_event_task_ids().await
    }

    fn validate_review_policy_create_gate(
        &self,
        params: &TaskCreateParams,
    ) -> TaskRuntimeResult<()> {
        let Some(review_policy) = params.agent_spec.as_ref().and_then(|spec| {
            spec.review_policy
                .as_ref()
                .filter(|policy| policy.is_enabled())
        }) else {
            return Ok(());
        };
        if !self.config.review.enabled || !self.config.review.allow_task_create_review_policy {
            bail!(
                "task review policy `{}` is not enabled for task_create",
                serde_json::to_value(review_policy.mode)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "unknown".to_owned())
            );
        }
        Ok(())
    }

    fn effective_agent_review_policy_for_create(
        &self,
        params: &TaskCreateParams,
        trigger_kind: TaskTriggerKind,
        lifecycle_policy: &TaskLifecyclePolicy,
    ) -> Option<TaskAgentReviewPolicy> {
        let explicit_policy = params
            .agent_spec
            .as_ref()
            .and_then(|spec| spec.review_policy.clone());
        if explicit_policy.is_some() {
            return explicit_policy;
        }
        if params.agent_spec.is_none()
            || params.executor_kind != TaskExecutorKind::Agent
            || trigger_kind != TaskTriggerKind::Immediate
            || lifecycle_policy.attachment != TaskAttachmentMode::Attached
            || !self.config.review.enabled
            || !self
                .config
                .review
                .default_parent_review_for_immediate_attached_agent_tasks
        {
            return None;
        }
        Some(TaskAgentReviewPolicy::parent_agent_default(
            self.config.review.default_max_revision_rounds,
        ))
    }

    pub async fn acquire_write_locks_for_run(
        &self,
        run_id: &str,
        now: i64,
    ) -> TaskRuntimeResult<WriteLockDecision> {
        let Some(run) = self.store.get_task_run(run_id).await? else {
            bail!("task run `{run_id}` not found");
        };
        let Some(response) = self.store.get_task(run.task_id.as_str()).await? else {
            bail!("task `{}` not found", run.task_id);
        };
        let Some(agent_spec) = response
            .agent_specs
            .iter()
            .rev()
            .find(|spec| spec.run_id.as_deref() == Some(run.id.as_str()))
            .or_else(|| {
                response
                    .agent_specs
                    .iter()
                    .rev()
                    .find(|spec| spec.run_id.is_none())
            })
        else {
            return Ok(WriteLockDecision::NoLocksRequired);
        };
        let scopes = write_lock_scopes(agent_spec)?;
        if scopes.is_empty() {
            return Ok(WriteLockDecision::NoLocksRequired);
        }

        let policy = response
            .task
            .concurrency_policy
            .as_ref()
            .map(|policy| policy.on_conflict)
            .unwrap_or(TaskConcurrencyConflictPolicy::Queue);
        if policy == TaskConcurrencyConflictPolicy::Allow {
            return Ok(WriteLockDecision::NoLocksRequired);
        }

        let existing_for_run = self
            .store
            .list_task_write_locks_by_run(run.id.as_str())
            .await?;
        if existing_for_run.iter().any(|lock| {
            lock.status == TaskWriteLockStatus::Acquired
                && lock.expires_at.is_none_or(|expires_at| expires_at > now)
        }) {
            return Ok(WriteLockDecision::Acquired(existing_for_run));
        }

        let conflicts = self
            .active_write_lock_conflicts(response.task.workspace_id.as_str(), &scopes, now)
            .await?;
        if !conflicts.is_empty() {
            match policy {
                TaskConcurrencyConflictPolicy::Reject => {
                    self.append_and_publish_lock_blocked(
                        response.task.id.as_str(),
                        run.id.as_str(),
                        conflicts,
                        now,
                    )
                    .await?;
                    return Ok(WriteLockDecision::Rejected);
                }
                TaskConcurrencyConflictPolicy::Queue => {
                    self.append_and_publish_lock_blocked(
                        response.task.id.as_str(),
                        run.id.as_str(),
                        conflicts,
                        now,
                    )
                    .await?;
                    return Ok(WriteLockDecision::Queued);
                }
                TaskConcurrencyConflictPolicy::CancelExisting => {
                    for conflict in conflicts {
                        let _ = self
                            .cancel_task(
                                TaskMutationContext::default(),
                                TaskCancelParams {
                                    task_id: conflict.task_id,
                                    reason: Some(format!(
                                        "cancelled by write-lock conflict with run {}",
                                        run.id
                                    )),
                                    scope: TaskCancelScope::TaskOnly,
                                },
                            )
                            .await;
                    }
                }
                TaskConcurrencyConflictPolicy::Allow => {}
            }
        }

        let expires_at = now.saturating_add(3600);
        let locks = scopes
            .into_iter()
            .map(|scope| TaskWriteLock {
                id: generate_id(ID_LEN),
                workspace_id: response.task.workspace_id.clone(),
                task_id: response.task.id.clone(),
                run_id: run.id.clone(),
                scope_kind: scope.kind,
                scope_path: scope.path,
                status: TaskWriteLockStatus::Acquired,
                acquired_at: now,
                expires_at: Some(expires_at),
                released_at: None,
                conflict_policy: policy,
                reason: None,
                created_at: now,
                updated_at: now,
            })
            .collect::<Vec<_>>();
        let events = locks
            .iter()
            .cloned()
            .map(|lock| TaskEventPayload::WriteLockAcquired { lock })
            .collect::<Vec<_>>();
        let appended = self.append_events(events, now).await?;
        self.publish_and_wake(appended).await;
        Ok(WriteLockDecision::Acquired(locks))
    }

    pub async fn recover_retry_and_lock_state(&self, now: i64) -> TaskRuntimeResult<usize> {
        let mut recovered = 0usize;
        let mut events = Vec::new();
        for mut lock in self.store.list_stale_task_write_locks(now, 1024).await? {
            if self
                .store
                .get_task_run(lock.run_id.as_str())
                .await?
                .is_some_and(|run| run.status == TaskRunStatus::WaitingReview)
            {
                lock.expires_at = None;
                lock.reason = Some("write lock held while task waits for review".to_owned());
                lock.updated_at = now;
                events.push(TaskEventPayload::WriteLockExtended {
                    lock,
                    extended_at: now,
                });
                recovered = recovered.saturating_add(1);
                continue;
            }
            lock.status = TaskWriteLockStatus::Expired;
            lock.released_at = Some(now);
            lock.reason = Some("write lock expired during startup recovery".to_owned());
            lock.updated_at = now;
            events.push(TaskEventPayload::WriteLockExpired {
                lock,
                expired_at: now,
            });
            recovered = recovered.saturating_add(1);
        }
        for run in self
            .store
            .list_task_runs_by_status(TaskRunStatus::Succeeded, 1024)
            .await?
            .into_iter()
            .chain(
                self.store
                    .list_task_runs_by_status(TaskRunStatus::Failed, 1024)
                    .await?
                    .into_iter(),
            )
            .chain(
                self.store
                    .list_task_runs_by_status(TaskRunStatus::Cancelled, 1024)
                    .await?
                    .into_iter(),
            )
        {
            for mut lock in self
                .store
                .list_task_write_locks_by_run(run.id.as_str())
                .await?
                .into_iter()
                .filter(|lock| lock.status == TaskWriteLockStatus::Acquired)
            {
                lock.status = TaskWriteLockStatus::Released;
                lock.released_at = Some(now);
                lock.reason = Some("terminal run lock released during startup recovery".to_owned());
                lock.updated_at = now;
                events.push(TaskEventPayload::WriteLockReleased {
                    lock,
                    released_at: now,
                });
                recovered = recovered.saturating_add(1);
            }
        }
        if !events.is_empty() {
            let appended = self.append_events(events, now).await?;
            self.publish_and_wake(appended).await;
        }
        Ok(recovered)
    }

    pub(crate) async fn append_event(
        &self,
        event: TaskEventPayload,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<AppendedTaskEvent> {
        self.projector
            .append_event(event, event_timestamp_secs)
            .await
    }

    pub(crate) async fn append_events(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<Vec<AppendedTaskEvent>> {
        self.projector
            .append_events(events, event_timestamp_secs)
            .await
    }

    pub(crate) async fn publish_and_wake(&self, events: Vec<AppendedTaskEvent>) {
        self.event_bus.publish_many(events).await;
        self.wake_scheduler().await;
    }

    async fn process_due_once(&self, now: i64) -> TaskRuntimeResult<usize> {
        let scheduler = self.scheduler.read().await.as_ref().and_then(Weak::upgrade);
        if let Some(scheduler) = scheduler {
            scheduler.process_due_once(now).await
        } else {
            Ok(0)
        }
    }

    async fn wake_scheduler(&self) {
        if let Some(scheduler) = self.scheduler.read().await.as_ref().and_then(Weak::upgrade) {
            scheduler.wake();
        }
    }

    async fn parent_context(
        &self,
        parent_task_id: Option<&str>,
    ) -> TaskRuntimeResult<ParentContext> {
        if let Some(parent_task_id) = parent_task_id {
            if let Some(parent) = self.store.get_task(parent_task_id).await? {
                let parent_spec = parent.agent_specs.last();
                let parent_depth = parent_spec.map(|spec| spec.depth).unwrap_or(0);
                let max_depth = parent_spec
                    .map(|spec| spec.max_depth)
                    .unwrap_or(DEFAULT_MAX_TASK_DEPTH);
                return Ok(ParentContext {
                    root_task_id: Some(
                        parent
                            .task
                            .root_task_id
                            .clone()
                            .unwrap_or_else(|| parent.task.id.clone()),
                    ),
                    depth: parent_depth.saturating_add(1),
                    max_depth,
                });
            }
            bail!("parent task `{parent_task_id}` not found");
        }
        Ok(ParentContext {
            root_task_id: None,
            depth: 1,
            max_depth: DEFAULT_MAX_TASK_DEPTH,
        })
    }

    async fn collect_wait_state(
        &self,
        params: &TaskWaitParams,
    ) -> TaskRuntimeResult<TaskWaitResponse> {
        let mut task_ids = params.task_ids.clone();
        for run_id in &params.run_ids {
            if let Some(run) = self.store.get_task_run(run_id).await?
                && !task_ids.iter().any(|task_id| task_id == &run.task_id)
            {
                task_ids.push(run.task_id);
            }
        }
        task_ids.sort();
        task_ids.dedup();

        let mut completed = Vec::new();
        let mut failed = Vec::new();
        let mut cancelled = Vec::new();
        let mut review_required = Vec::new();
        let mut pending = Vec::new();
        let mut total_count = 0u32;
        let mut terminal_count = 0u32;
        let mut review_required_count = 0u32;
        let mut pending_count = 0u32;

        for task_id in task_ids {
            let Some(response) = self.store.get_task(task_id.as_str()).await? else {
                total_count = total_count.saturating_add(1);
                terminal_count = terminal_count.saturating_add(1);
                continue;
            };
            let run = select_wait_run(&response.runs, &params.run_ids);
            let child_anchor = match run.as_ref() {
                Some(run) => {
                    self.store
                        .get_task_run_child_anchor(run.id.as_str())
                        .await?
                }
                None => Default::default(),
            };
            let item = TaskWaitItem {
                task: response.task.clone(),
                run: run.clone(),
                child_thread_id: child_anchor.child_thread_id,
                child_turn_id: child_anchor.child_turn_id,
            };
            total_count = total_count.saturating_add(1);
            match wait_item_state(&item) {
                WaitItemState::Completed => {
                    terminal_count = terminal_count.saturating_add(1);
                    if params.return_completed {
                        completed.push(item);
                    }
                }
                WaitItemState::Failed => {
                    terminal_count = terminal_count.saturating_add(1);
                    failed.push(item);
                }
                WaitItemState::Cancelled => {
                    terminal_count = terminal_count.saturating_add(1);
                    cancelled.push(item);
                }
                WaitItemState::ReviewRequired => {
                    if let Some(review_item) = self
                        .review_required_wait_item(&response, item.clone())
                        .await?
                    {
                        review_required_count = review_required_count.saturating_add(1);
                        review_required.push(review_item);
                    } else {
                        pending_count = pending_count.saturating_add(1);
                        if params.return_pending {
                            pending.push(item);
                        }
                    }
                }
                WaitItemState::Pending => {
                    pending_count = pending_count.saturating_add(1);
                    if params.return_pending {
                        pending.push(item);
                    }
                }
            }
        }

        Ok(TaskWaitResponse {
            completed,
            failed,
            cancelled,
            review_required,
            pending,
            non_waitable: Vec::new(),
            timed_out: false,
            total_count,
            terminal_count,
            pending_count,
            review_required_count,
            non_waitable_count: 0,
            mode: params.mode,
        })
    }

    async fn review_required_wait_item(
        &self,
        response: &TaskGetResponse,
        item: TaskWaitItem,
    ) -> TaskRuntimeResult<Option<TaskWaitReviewItem>> {
        let Some(run) = item.run.as_ref() else {
            return Ok(None);
        };
        let Some(candidate) = self
            .active_review_candidate_for_run(run.id.as_str())
            .await?
        else {
            return Ok(None);
        };
        let max_revision_rounds = wait_review_policy(response, run)
            .map(|policy| policy.max_revision_rounds)
            .unwrap_or_default();
        let remaining_revision_rounds = max_revision_rounds.saturating_sub(candidate.round);
        let allowed_actions =
            wait_review_allowed_actions(candidate.status, remaining_revision_rounds);
        let revision_blocked_reason =
            wait_review_revision_blocked_reason(candidate.status, remaining_revision_rounds);

        Ok(Some(TaskWaitReviewItem {
            item,
            candidate,
            max_revision_rounds,
            remaining_revision_rounds,
            allowed_actions,
            revision_blocked_reason,
        }))
    }

    async fn build_wait_target_plan(
        &self,
        params: &TaskWaitParams,
    ) -> TaskRuntimeResult<WaitTargetPlan> {
        let now = now_timestamp_secs();
        let mut wait_params = params.clone();
        let mut wait_task_ids = Vec::with_capacity(params.task_ids.len());
        let mut non_waitable = Vec::new();

        for task_id in &params.task_ids {
            if let Some(item) = self
                .non_waitable_scheduled_task_without_active_run(task_id.as_str(), now)
                .await?
            {
                non_waitable.push(item);
            } else {
                wait_task_ids.push(task_id.clone());
            }
        }

        wait_params.task_ids = wait_task_ids;

        Ok(WaitTargetPlan {
            wait_params,
            non_waitable,
        })
    }

    async fn collect_wait_state_for_plan(
        &self,
        plan: &WaitTargetPlan,
    ) -> TaskRuntimeResult<TaskWaitResponse> {
        let mut response = if has_wait_targets(&plan.wait_params) {
            self.collect_wait_state(&plan.wait_params).await?
        } else {
            empty_wait_response(plan.wait_params.mode)
        };

        response.non_waitable = plan.non_waitable.clone();
        response.non_waitable_count = usize_to_u32(response.non_waitable.len());
        response.total_count = response
            .total_count
            .saturating_add(response.non_waitable_count);

        Ok(response)
    }

    async fn non_waitable_scheduled_task_without_active_run(
        &self,
        task_id: &str,
        now: i64,
    ) -> TaskRuntimeResult<Option<TaskWaitNonWaitableItem>> {
        let Some(response) = self.store.get_task(task_id).await? else {
            return Ok(None);
        };
        if response.task.status != TaskStatus::Scheduled {
            return Ok(None);
        }
        if response.runs.iter().any(|run| !run.status.is_terminal()) {
            return Ok(None);
        }
        let next_fire_at = response
            .triggers
            .iter()
            .filter(|trigger| {
                trigger.status == TaskTriggerStatus::Active
                    && matches!(
                        trigger.kind(),
                        TaskTriggerKind::ScheduledAt
                            | TaskTriggerKind::Interval
                            | TaskTriggerKind::Cron
                    )
            })
            .filter_map(|trigger| trigger.next_fire_at)
            .filter(|next_fire_at| *next_fire_at > now)
            .min();

        let Some(next_fire_at) = next_fire_at else {
            return Ok(None);
        };

        Ok(Some(TaskWaitNonWaitableItem {
            item: TaskWaitItem {
                task: response.task,
                run: None,
                child_thread_id: None,
                child_turn_id: None,
            },
            reason: TaskWaitNonWaitableReason::FutureScheduledTaskWithoutActiveRun,
            next_fire_at: Some(next_fire_at),
        }))
    }

    async fn push_cancel_task_events(
        &self,
        response: &TaskGetResponse,
        reason: &str,
        now: i64,
        events: &mut Vec<TaskEventPayload>,
        cancelled_runs: &mut Vec<TaskRun>,
        cancelled_deliveries: &mut Vec<TaskDelivery>,
        cancelled_executions: &mut Vec<(String, Option<TaskError>)>,
    ) -> TaskRuntimeResult<()> {
        if is_terminal_task(response.task.status) {
            return Ok(());
        }
        for run in response
            .runs
            .iter()
            .filter(|run| !is_terminal_run(run.status))
        {
            if run.status == TaskRunStatus::WaitingReview {
                self.push_cancel_waiting_review_run_events(
                    response,
                    run,
                    reason,
                    now,
                    events,
                    cancelled_runs,
                    cancelled_executions,
                )
                .await?;
                continue;
            }
            if let Some(executor) = self.executors.get(run.executor_kind).await {
                let handle = TaskExecutionHandle::new(
                    self.store.clone(),
                    self.event_bus.clone(),
                    response.task.id.clone(),
                    run.id.clone(),
                );
                let _ = executor
                    .cancel_run(
                        TaskExecutionContext {
                            workspace_id: response.task.workspace_id.clone(),
                            task_id: response.task.id.clone(),
                            execution_id: None,
                            worker_id: format!("task-cancel-{}", generate_id(ID_LEN)),
                        },
                        run.id.as_str(),
                        reason,
                        handle,
                    )
                    .await;
            }
            if let Some(current_run) = self.store.get_task_run(run.id.as_str()).await?
                && is_terminal_run(current_run.status)
            {
                self.push_cancelled_write_locks_for_run(run.id.as_str(), reason, now, events)
                    .await?;
                continue;
            }
            events.push(TaskEventPayload::RunCancelled {
                task_id: response.task.id.clone(),
                run_id: run.id.clone(),
                reason: Some(reason.to_owned()),
                cancelled_at: now,
            });
            cancelled_runs.push(run.clone());
            self.push_cancelled_write_locks_for_run(run.id.as_str(), reason, now, events)
                .await?;
        }
        for trigger in response
            .triggers
            .iter()
            .filter(|trigger| trigger.status == TaskTriggerStatus::Active)
        {
            let mut trigger = trigger.clone();
            trigger.status = TaskTriggerStatus::Cancelled;
            trigger.next_fire_at = None;
            trigger.updated_at = now;
            events.push(TaskEventPayload::TaskRescheduled {
                task_id: response.task.id.clone(),
                trigger,
                rescheduled_at: now,
                reason: pioneer_protocol::TaskRescheduleReason::TaskCancelled,
            });
        }
        for mut delivery in self
            .store
            .list_task_deliveries(TaskDeliveriesParams {
                workspace_id: response.task.workspace_id.clone(),
                task_id: Some(response.task.id.clone()),
                run_id: None,
                statuses: vec![TaskDeliveryStatus::Pending, TaskDeliveryStatus::Delivering],
                limit: Some(1000),
            })
            .await?
            .deliveries
        {
            delivery.status = TaskDeliveryStatus::Cancelled;
            delivery.next_attempt_at = None;
            delivery.last_error = Some(reason.to_owned());
            delivery.updated_at = now;
            events.push(TaskEventPayload::DeliveryCancelled {
                delivery: delivery.clone(),
                reason: Some(reason.to_owned()),
            });
            cancelled_deliveries.push(delivery);
        }
        if let Some(current_response) = self.store.get_task(response.task.id.as_str()).await?
            && is_terminal_task(current_response.task.status)
        {
            return Ok(());
        }
        events.push(TaskEventPayload::TaskCancelled {
            task_id: response.task.id.clone(),
            reason: Some(reason.to_owned()),
            completed_at: now,
        });
        Ok(())
    }

    async fn push_cancel_waiting_review_run_events(
        &self,
        response: &TaskGetResponse,
        run: &TaskRun,
        reason: &str,
        now: i64,
        events: &mut Vec<TaskEventPayload>,
        cancelled_runs: &mut Vec<TaskRun>,
        cancelled_executions: &mut Vec<(String, Option<TaskError>)>,
    ) -> TaskRuntimeResult<()> {
        if let Some(candidate) = self
            .active_review_candidate_for_run(run.id.as_str())
            .await?
        {
            let review_event = cancel_review_event_for_candidate(&candidate, reason, now);
            let review_event_id = review_event.id.clone();
            let mut cancelled_candidate = candidate;
            cancelled_candidate.status = TaskResultCandidateStatus::Cancelled;
            cancelled_candidate.final_review_event_id = Some(review_event_id.clone());
            cancelled_candidate.updated_at = now;
            cancelled_candidate.resolved_at = Some(now);
            events.push(TaskEventPayload::TaskResultReviewEventRecorded { review_event });
            events.push(TaskEventPayload::TaskResultCandidateCancelled {
                candidate: cancelled_candidate,
                review_event_id,
            });
        }

        events.push(TaskEventPayload::RunCancelled {
            task_id: response.task.id.clone(),
            run_id: run.id.clone(),
            reason: Some(reason.to_owned()),
            cancelled_at: now,
        });
        cancelled_runs.push(run.clone());
        self.push_cancelled_write_locks_for_run(run.id.as_str(), reason, now, events)
            .await?;
        if let Some(execution) = self.store.load_execution_for_run(run.id.as_str()).await?
            && !execution.status.is_terminal()
        {
            cancelled_executions.push((
                execution.id,
                Some(TaskError {
                    code: "task_run_cancelled".to_owned(),
                    message: reason.to_owned(),
                    class: TaskErrorClass::Cancelled,
                    details: None,
                    failed_run_id: Some(run.id.clone()),
                }),
            ));
        }
        Ok(())
    }

    async fn active_review_candidate_for_run(
        &self,
        run_id: &str,
    ) -> TaskRuntimeResult<Option<TaskResultCandidate>> {
        let mut candidates = self.store.list_task_result_candidates(run_id).await?;
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
        Ok(candidates.into_iter().next())
    }

    async fn push_cancelled_write_locks_for_run(
        &self,
        run_id: &str,
        reason: &str,
        now: i64,
        events: &mut Vec<TaskEventPayload>,
    ) -> TaskRuntimeResult<()> {
        for mut lock in self
            .store
            .list_task_write_locks_by_run(run_id)
            .await?
            .into_iter()
            .filter(|lock| {
                matches!(
                    lock.status,
                    TaskWriteLockStatus::Pending
                        | TaskWriteLockStatus::Acquired
                        | TaskWriteLockStatus::Blocked
                )
            })
        {
            lock.status = TaskWriteLockStatus::Cancelled;
            lock.released_at = Some(now);
            lock.reason = Some(reason.to_owned());
            lock.updated_at = now;
            events.push(TaskEventPayload::WriteLockReleased {
                lock,
                released_at: now,
            });
        }
        Ok(())
    }

    async fn active_write_lock_conflicts(
        &self,
        workspace_id: &str,
        scopes: &[WriteLockScope],
        now: i64,
    ) -> TaskRuntimeResult<Vec<TaskWriteLockConflict>> {
        let mut conflicts = Vec::new();
        for lock in self
            .store
            .list_active_task_write_locks(workspace_id, now, 4096)
            .await?
        {
            if scopes.iter().any(|scope| {
                write_lock_paths_overlap(scope.path.as_str(), lock.scope_path.as_str())
            }) {
                conflicts.push(TaskWriteLockConflict {
                    lock_id: lock.id,
                    task_id: lock.task_id,
                    run_id: lock.run_id,
                    scope_kind: lock.scope_kind,
                    scope_path: lock.scope_path,
                });
            }
        }
        Ok(conflicts)
    }

    async fn append_and_publish_lock_blocked(
        &self,
        task_id: &str,
        run_id: &str,
        conflicts: Vec<TaskWriteLockConflict>,
        now: i64,
    ) -> TaskRuntimeResult<()> {
        let appended = self
            .append_event(
                TaskEventPayload::WriteLockBlocked {
                    task_id: task_id.to_owned(),
                    run_id: run_id.to_owned(),
                    conflicts,
                    blocked_at: now,
                },
                now,
            )
            .await?;
        self.publish_and_wake(vec![appended]).await;
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ParentContext {
    root_task_id: Option<String>,
    depth: i64,
    max_depth: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WriteLockDecision {
    NoLocksRequired,
    Acquired(Vec<TaskWriteLock>),
    Queued,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WriteLockScope {
    kind: TaskWriteLockScopeKind,
    path: String,
}

#[derive(Debug, Clone, Default)]
struct CancellationPlan {
    cancelled_tasks: Vec<Task>,
    detached_tasks: Vec<Task>,
    kept_tasks: Vec<Task>,
}

fn plan_cancellation(tree: &TaskTree, scope: TaskCancelScope) -> CancellationPlan {
    let mut plan = CancellationPlan::default();
    match scope {
        TaskCancelScope::TaskOnly => {
            plan.cancelled_tasks.push(tree.task.clone());
            for child in &tree.children {
                collect_kept_subtree(child, &mut plan);
            }
        }
        TaskCancelScope::FullSubtree => collect_cancelled_subtree(tree, &mut plan),
        TaskCancelScope::AttachedSubtree => {
            plan.cancelled_tasks.push(tree.task.clone());
            for child in &tree.children {
                plan_attached_descendant(child, &mut plan);
            }
        }
    }
    plan.cancelled_tasks.reverse();
    plan
}

fn collect_cancelled_subtree(tree: &TaskTree, plan: &mut CancellationPlan) {
    plan.cancelled_tasks.push(tree.task.clone());
    for child in &tree.children {
        collect_cancelled_subtree(child, plan);
    }
}

fn collect_kept_subtree(tree: &TaskTree, plan: &mut CancellationPlan) {
    plan.kept_tasks.push(tree.task.clone());
    for child in &tree.children {
        collect_kept_subtree(child, plan);
    }
}

fn plan_attached_descendant(tree: &TaskTree, plan: &mut CancellationPlan) {
    let lifecycle = effective_lifecycle_policy(&tree.task);
    if lifecycle.attachment != TaskAttachmentMode::Attached {
        collect_kept_subtree(tree, plan);
        return;
    }
    match lifecycle.on_parent_cancel {
        TaskParentTerminalAction::Cancel => {
            plan.cancelled_tasks.push(tree.task.clone());
            for child in &tree.children {
                plan_attached_descendant(child, plan);
            }
        }
        TaskParentTerminalAction::Detach => {
            plan.detached_tasks.push(tree.task.clone());
            for child in &tree.children {
                collect_kept_subtree(child, plan);
            }
        }
        TaskParentTerminalAction::KeepRunning => collect_kept_subtree(tree, plan),
    }
}

fn effective_lifecycle_policy(task: &Task) -> TaskLifecyclePolicy {
    task.lifecycle_policy
        .clone()
        .unwrap_or_else(|| default_lifecycle_policy(TaskTriggerKind::Immediate, false))
}

fn write_lock_scopes(agent_spec: &TaskAgentSpec) -> TaskRuntimeResult<Vec<WriteLockScope>> {
    let Some(policy) = agent_spec.tool_policy.as_ref() else {
        return Ok(Vec::new());
    };
    match policy.write_mode {
        TaskAgentWriteMode::ReadOnly => Ok(Vec::new()),
        TaskAgentWriteMode::WorkspaceWrite | TaskAgentWriteMode::FullAccess => {
            Ok(vec![WriteLockScope {
                kind: TaskWriteLockScopeKind::Workspace,
                path: ".".to_owned(),
            }])
        }
        TaskAgentWriteMode::ScopedWrite => {
            if policy.allowed_paths.is_empty() {
                bail!("scoped write task requires allowed_paths");
            }
            policy
                .allowed_paths
                .iter()
                .map(|path| {
                    Ok(WriteLockScope {
                        kind: TaskWriteLockScopeKind::Path,
                        path: normalize_workspace_relative_path(path)?,
                    })
                })
                .collect()
        }
    }
}

fn normalize_workspace_relative_path(path: &str) -> TaskRuntimeResult<String> {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "." {
        return Ok(".".to_owned());
    }
    let path = Path::new(trimmed);
    if path.is_absolute() {
        bail!("write lock path `{trimmed}` must be workspace-relative");
    }
    let mut parts = VecDeque::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => {
                let Some(value) = value.to_str() else {
                    bail!("write lock path `{trimmed}` is not valid UTF-8");
                };
                parts.push_back(value.to_owned());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                if parts.pop_back().is_none() {
                    bail!("write lock path `{trimmed}` escapes the workspace");
                }
            }
            Component::RootDir | Component::Prefix(_) => {
                bail!("write lock path `{trimmed}` must be workspace-relative");
            }
        }
    }
    if parts.is_empty() {
        Ok(".".to_owned())
    } else {
        Ok(parts.into_iter().collect::<Vec<_>>().join("/"))
    }
}

fn write_lock_paths_overlap(left: &str, right: &str) -> bool {
    if left == "." || right == "." {
        return true;
    }
    let left_parts = left.split('/').collect::<Vec<_>>();
    let right_parts = right.split('/').collect::<Vec<_>>();
    left_parts.starts_with(right_parts.as_slice()) || right_parts.starts_with(left_parts.as_slice())
}

fn validate_create_params(params: &TaskCreateParams) -> TaskRuntimeResult<()> {
    if params.workspace_id.trim().is_empty() {
        bail!("workspace_id is required");
    }
    required_trimmed(&params.title, "title")?;
    required_trimmed(&params.goal, "goal")?;
    if params.executor_kind == TaskExecutorKind::Agent {
        let agent_spec = params
            .agent_spec
            .as_ref()
            .ok_or_else(|| anyhow!("agent executor requires agent_spec"))?;
        if agent_spec
            .model
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
        {
            bail!("agent executor requires agent_spec.model");
        }
        if agent_spec
            .model_provider
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
        {
            bail!("agent executor requires agent_spec.model_provider");
        }
        validate_agent_spec_for_trigger(params.trigger.spec.kind(), agent_spec)?;
    }
    if params.owner_kind == TaskOwnerKind::Thread && params.owner_id.is_none() {
        bail!("thread-owned task requires owner_id");
    }
    if let Some(policy) = params.delivery_policy.as_ref() {
        validate_delivery_policy(policy)?;
    }
    Ok(())
}

fn validate_update_params(params: &TaskUpdateParams) -> TaskRuntimeResult<()> {
    required_trimmed(params.task_id.as_str(), "task_id")?;
    if params.clear_input && (params.input_text.is_some() || params.input.is_some()) {
        bail!("task update cannot clear and set input in the same request");
    }
    if params.clear_agent_role && params.agent_role.is_some() {
        bail!("task update cannot clear and set agent_role in the same request");
    }
    if params.clear_agent_nickname && params.agent_nickname.is_some() {
        bail!("task update cannot clear and set agent_nickname in the same request");
    }
    if params.clear_output_instructions && params.output_instructions.is_some() {
        bail!("task update cannot clear and set output_instructions in the same request");
    }
    if params.clear_context_policy && params.context_policy.is_some() {
        bail!("task update cannot clear and set context_policy in the same request");
    }
    if params.clear_tool_policy && params.tool_policy.is_some() {
        bail!("task update cannot clear and set tool_policy in the same request");
    }
    if params.clear_result_contract && params.result_contract.is_some() {
        bail!("task update cannot clear and set result_contract in the same request");
    }
    if params.clear_timeout_policy && params.timeout_policy.is_some() {
        bail!("task update cannot clear and set timeout_policy in the same request");
    }
    if params.clear_concurrency_policy && params.concurrency_policy.is_some() {
        bail!("task update cannot clear and set concurrency_policy in the same request");
    }
    if params.clear_metadata && params.metadata.is_some() {
        bail!("task update cannot clear and set metadata in the same request");
    }
    Ok(())
}

fn update_has_agent_patch(params: &TaskUpdateParams) -> bool {
    params.agent_role.is_some()
        || params.agent_nickname.is_some()
        || params.instructions.is_some()
        || params.input_text.is_some()
        || params.input.is_some()
        || params.output_instructions.is_some()
        || params.context_policy.is_some()
        || params.tool_policy.is_some()
        || params.result_contract.is_some()
        || params.clear_agent_role
        || params.clear_agent_nickname
        || params.clear_input
        || params.clear_output_instructions
        || params.clear_context_policy
        || params.clear_tool_policy
        || params.clear_result_contract
}

fn validate_agent_spec_for_trigger(
    trigger_kind: TaskTriggerKind,
    agent_spec: &pioneer_protocol::TaskAgentSpecInput,
) -> TaskRuntimeResult<()> {
    validate_agent_prompt_for_trigger(trigger_kind, &agent_spec.prompt)
}

fn validate_agent_prompt_for_trigger(
    trigger_kind: TaskTriggerKind,
    prompt: &TaskAgentPrompt,
) -> TaskRuntimeResult<()> {
    if !matches!(
        trigger_kind,
        TaskTriggerKind::ScheduledAt | TaskTriggerKind::Interval | TaskTriggerKind::Cron
    ) {
        return Ok(());
    }

    let instructions = prompt
        .instructions
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if instructions.is_empty() {
        bail!("scheduled agent task requires self-contained executor instructions");
    }
    if instructions.len() == 1 && instructions[0] == "Return a concise final result." {
        bail!("scheduled agent task cannot use the generic concise-result prompt");
    }
    if prompt
        .output_instructions
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_none()
    {
        bail!("scheduled agent task requires output instructions");
    }
    Ok(())
}

fn normalize_agent_instructions(instructions: Vec<String>) -> Vec<String> {
    let instructions = instructions
        .into_iter()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    let mut normalized = Vec::new();
    for instruction in instructions {
        if !normalized.iter().any(|value| value == &instruction) {
            normalized.push(instruction);
        }
    }
    normalized
}

fn merge_agent_input(
    input_text: Option<String>,
    input: Option<pioneer_protocol::TaskAgentInput>,
) -> TaskRuntimeResult<Option<pioneer_protocol::TaskAgentInput>> {
    let input_text = input_text
        .map(|value| required_trimmed(value.as_str(), "inputText"))
        .transpose()?;
    let mut input = input.map(normalize_agent_input);
    if let Some(input_text) = input_text {
        match input.as_mut() {
            Some(input) => match input.text.as_deref() {
                Some(existing) if existing == input_text => {}
                Some(existing) => {
                    bail!(
                        "input_text conflicts with input.text: got different values `{}` and `{}`",
                        input_text,
                        existing
                    );
                }
                None => input.text = Some(input_text),
            },
            None => {
                input = Some(pioneer_protocol::TaskAgentInput {
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

fn normalize_agent_input(
    mut input: pioneer_protocol::TaskAgentInput,
) -> pioneer_protocol::TaskAgentInput {
    input.text = input
        .text
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty());
    input
}

fn clean_required_optional(value: String, field: &str) -> TaskRuntimeResult<Option<String>> {
    Ok(Some(required_trimmed(value.as_str(), field)?))
}

fn push_changed(changed_fields: &mut Vec<String>, field: &str) {
    if !changed_fields.iter().any(|value| value == field) {
        changed_fields.push(field.to_owned());
    }
}

fn set_changed<T>(slot: &mut T, value: T, field: &str, changed_fields: &mut Vec<String>)
where
    T: PartialEq,
{
    if *slot != value {
        *slot = value;
        push_changed(changed_fields, field);
    }
}

fn set_option_changed<T>(
    slot: &mut Option<T>,
    value: Option<T>,
    field: &str,
    changed_fields: &mut Vec<String>,
) where
    T: PartialEq,
{
    if *slot != value {
        *slot = value;
        push_changed(changed_fields, field);
    }
}

fn validate_delivery_policy(
    policy: &pioneer_protocol::TaskDeliveryPolicy,
) -> TaskRuntimeResult<()> {
    match policy.mode {
        TaskDeliveryMode::None
        | TaskDeliveryMode::OwnerThread
        | TaskDeliveryMode::UserNotification => {}
        TaskDeliveryMode::Thread => {
            if policy
                .thread_id
                .as_deref()
                .map(str::trim)
                .filter(|thread_id| !thread_id.is_empty())
                .is_none()
            {
                bail!("thread delivery requires delivery_policy.thread_id");
            }
        }
        TaskDeliveryMode::Webhook => {
            let Some(url) = policy
                .webhook_url
                .as_deref()
                .map(str::trim)
                .filter(|url| !url.is_empty())
            else {
                bail!("webhook delivery requires delivery_policy.webhook_url");
            };
            if !url.starts_with("https://") {
                bail!("webhook delivery requires an https:// URL");
            }
        }
    }
    Ok(())
}

fn normalize_max_depth(
    depth: i64,
    parent_max_depth: i64,
    agent_spec: Option<&pioneer_protocol::TaskAgentSpecInput>,
) -> TaskRuntimeResult<i64> {
    let requested = agent_spec
        .map(|spec| spec.max_depth)
        .unwrap_or(parent_max_depth);
    if requested < 0 {
        bail!("max_depth must be non-negative");
    }
    if depth > 0 && requested > parent_max_depth {
        bail!("child task cannot raise inherited max_depth");
    }
    Ok(requested.min(MAX_ROOT_TASK_DEPTH_LIMIT))
}

fn resume_next_fire_at(trigger: &TaskTrigger, now: i64) -> TaskRuntimeResult<Option<i64>> {
    match &trigger.spec {
        pioneer_protocol::TaskTriggerSpec::ScheduledAt { scheduled_at, .. } => {
            if *scheduled_at <= now {
                bail!(
                    "scheduled task `{}` missed its one-shot fire time; reschedule before resume",
                    trigger.task_id
                );
            }
            Ok(Some(*scheduled_at))
        }
        pioneer_protocol::TaskTriggerSpec::Interval { .. }
        | pioneer_protocol::TaskTriggerSpec::Cron { .. } => {
            TaskTriggerCalculator::initial_next_fire_at(&trigger.spec, now)
        }
        pioneer_protocol::TaskTriggerSpec::Immediate => Ok(Some(now)),
        pioneer_protocol::TaskTriggerSpec::Manual { .. }
        | pioneer_protocol::TaskTriggerSpec::External { .. }
        | pioneer_protocol::TaskTriggerSpec::Dependency { .. } => Ok(None),
    }
}

fn required_trimmed(value: &str, field: &str) -> TaskRuntimeResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("`{field}` is required");
    }
    Ok(trimmed.to_owned())
}

fn cancel_review_event_for_candidate(
    candidate: &TaskResultCandidate,
    reason: &str,
    created_at: i64,
) -> TaskResultReviewEvent {
    TaskResultReviewEvent {
        id: format!("task_result_review_cancel_{}", candidate.id),
        candidate_id: candidate.id.clone(),
        task_id: candidate.task_id.clone(),
        run_id: candidate.run_id.clone(),
        task_run_turn_id: candidate.task_run_turn_id.clone(),
        reviewer_kind: TaskResultReviewerKind::System,
        reviewer_thread_id: None,
        reviewer_turn_id: None,
        reviewer_user_id: None,
        reviewer_agent_spec_id: None,
        event_kind: TaskResultReviewEventKind::Decision,
        decision: TaskResultReviewDecision::Cancel,
        feedback_text: Some(reason.to_owned()),
        feedback: None,
        confidence: None,
        supersedes_review_event_id: candidate.final_review_event_id.clone(),
        next_task_run_turn_id: None,
        created_at,
    }
}

pub(crate) fn now_timestamp_secs() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

pub(crate) fn is_terminal_task(status: TaskStatus) -> bool {
    status.is_terminal()
}

pub(crate) fn is_terminal_run(status: TaskRunStatus) -> bool {
    status.is_terminal()
}

fn select_wait_run(runs: &[TaskRun], run_ids: &[String]) -> Option<TaskRun> {
    if run_ids.is_empty() {
        return runs.last().cloned();
    }
    runs.iter()
        .rev()
        .find(|run| run_ids.iter().any(|run_id| run_id == &run.id))
        .cloned()
}

#[derive(Debug, Clone)]
struct WaitTargetPlan {
    wait_params: TaskWaitParams,
    non_waitable: Vec<TaskWaitNonWaitableItem>,
}

fn has_wait_targets(params: &TaskWaitParams) -> bool {
    !params.task_ids.is_empty() || !params.run_ids.is_empty()
}

fn empty_wait_response(mode: TaskWaitMode) -> TaskWaitResponse {
    TaskWaitResponse {
        completed: Vec::new(),
        failed: Vec::new(),
        cancelled: Vec::new(),
        review_required: Vec::new(),
        pending: Vec::new(),
        non_waitable: Vec::new(),
        timed_out: false,
        total_count: 0,
        terminal_count: 0,
        pending_count: 0,
        review_required_count: 0,
        non_waitable_count: 0,
        mode,
    }
}

fn usize_to_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitItemState {
    Completed,
    Failed,
    Cancelled,
    ReviewRequired,
    Pending,
}

fn wait_item_state(item: &TaskWaitItem) -> WaitItemState {
    if let Some(run) = item.run.as_ref() {
        match run.status {
            TaskRunStatus::Succeeded => return WaitItemState::Completed,
            TaskRunStatus::Failed | TaskRunStatus::TimedOut => return WaitItemState::Failed,
            TaskRunStatus::Cancelled => return WaitItemState::Cancelled,
            TaskRunStatus::WaitingReview => return WaitItemState::ReviewRequired,
            _ => {}
        }
    }

    match item.task.status {
        TaskStatus::Completed => WaitItemState::Completed,
        TaskStatus::Failed => WaitItemState::Failed,
        TaskStatus::Cancelled => WaitItemState::Cancelled,
        TaskStatus::WaitingReview => WaitItemState::ReviewRequired,
        _ => WaitItemState::Pending,
    }
}

fn wait_review_policy<'a>(
    response: &'a TaskGetResponse,
    run: &TaskRun,
) -> Option<&'a TaskAgentReviewPolicy> {
    response
        .agent_specs
        .iter()
        .find(|spec| spec.run_id.as_deref() == Some(run.id.as_str()))
        .and_then(|spec| spec.review_policy.as_ref())
        .or_else(|| {
            response
                .agent_specs
                .iter()
                .find(|spec| spec.run_id.is_none())
                .and_then(|spec| spec.review_policy.as_ref())
        })
        .filter(|policy| policy.is_enabled())
}

fn wait_review_allowed_actions(
    candidate_status: TaskResultCandidateStatus,
    remaining_revision_rounds: u32,
) -> Vec<TaskWaitReviewAction> {
    let mut actions = Vec::new();
    if candidate_status == TaskResultCandidateStatus::PendingReview {
        actions.push(TaskWaitReviewAction::TaskAccept);
    }
    if wait_review_candidate_can_revise(candidate_status) && remaining_revision_rounds > 0 {
        actions.push(TaskWaitReviewAction::TaskRevise);
    }
    actions.push(TaskWaitReviewAction::TaskCancel);
    actions
}

fn wait_review_revision_blocked_reason(
    candidate_status: TaskResultCandidateStatus,
    remaining_revision_rounds: u32,
) -> Option<TaskWaitRevisionBlockedReason> {
    if !wait_review_candidate_can_revise(candidate_status) {
        return Some(TaskWaitRevisionBlockedReason::CandidateNotRevisable);
    }
    (remaining_revision_rounds == 0)
        .then_some(TaskWaitRevisionBlockedReason::MaxRevisionRoundsReached)
}

fn wait_review_candidate_can_revise(candidate_status: TaskResultCandidateStatus) -> bool {
    matches!(
        candidate_status,
        TaskResultCandidateStatus::PendingReview | TaskResultCandidateStatus::ExtractionFailed
    )
}

fn wait_condition_satisfied(response: &TaskWaitResponse) -> bool {
    let waitable_total = response
        .total_count
        .saturating_sub(response.non_waitable_count);
    match response.mode {
        TaskWaitMode::AllTerminal => {
            response.pending_count == 0 && response.review_required_count == 0
        }
        TaskWaitMode::AnyTerminal => response.terminal_count > 0 || waitable_total == 0,
        TaskWaitMode::AllTerminalOrReviewRequired => response.pending_count == 0,
        TaskWaitMode::AnyTerminalOrReviewRequired => {
            response.terminal_count > 0 || response.review_required_count > 0 || waitable_total == 0
        }
    }
}

pub(crate) fn task_error(
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
