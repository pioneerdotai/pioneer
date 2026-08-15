use pioneer_protocol::{
    TaskAttachmentMode, TaskCompletionBehavior, TaskDeliveryFormat, TaskDeliveryMode,
    TaskDeliveryPolicy, TaskDeliveryThreadTarget, TaskLifecyclePolicy, TaskOwnerKind,
    TaskParentTerminalAction, TaskRetryBackoffKind, TaskRetryPolicy, TaskTriggerKind,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskRunConversationSnapshotSeed {
    pub conversation_thread_id: String,
    pub source_turn_id: Option<String>,
    pub history_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskExecutionAdmissionSeed {
    pub workspace_id: String,
    pub root_thread_id: String,
    pub initiating_principal_id: String,
    pub authorization_context_json: String,
    pub role_key: String,
    pub policy_fingerprint: String,
    pub execution_resources: pioneer_crud::ExecutionAdmissionQuotaPolicy,
    pub task_resources: pioneer_protocol::TaskResourceBudget,
}

#[derive(Debug, Clone, Default)]
pub struct TaskCreateContext {
    pub actor_id: Option<String>,
    pub conversation_snapshot: Option<TaskRunConversationSnapshotSeed>,
    pub execution_admission: Option<TaskExecutionAdmissionSeed>,
    pub task_resource_budget: Option<pioneer_protocol::TaskResourceBudget>,
}

#[derive(Debug, Clone, Default)]
pub struct TaskMutationContext {
    pub actor_id: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    /// Gateway-validated authority used only when explicitly reopening a
    /// blocked Agent Task. Ordinary mutations and non-Agent Tasks leave this
    /// empty.
    pub execution_admission: Option<TaskExecutionAdmissionSeed>,
    pub task_resource_budget: Option<pioneer_protocol::TaskResourceBudget>,
}

impl TaskMutationContext {
    pub fn parent_agent(thread_id: impl Into<String>, turn_id: impl Into<String>) -> Self {
        Self {
            actor_id: None,
            thread_id: Some(thread_id.into()),
            turn_id: Some(turn_id.into()),
            execution_admission: None,
            task_resource_budget: None,
        }
    }

    pub fn user(actor_id: impl Into<String>) -> Self {
        Self {
            actor_id: Some(actor_id.into()),
            thread_id: None,
            turn_id: None,
            execution_admission: None,
            task_resource_budget: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TaskWaitContext {
    pub actor_id: Option<String>,
    pub task_resource_budget: Option<pioneer_protocol::TaskResourceBudget>,
}

pub fn default_lifecycle_policy(
    trigger_kind: TaskTriggerKind,
    created_by_turn: bool,
) -> TaskLifecyclePolicy {
    let attached = trigger_kind == TaskTriggerKind::Immediate && created_by_turn;
    TaskLifecyclePolicy {
        attachment: if attached {
            TaskAttachmentMode::Attached
        } else {
            TaskAttachmentMode::Detached
        },
        on_parent_cancel: if attached {
            TaskParentTerminalAction::Cancel
        } else {
            TaskParentTerminalAction::KeepRunning
        },
        on_parent_failure: if attached {
            TaskParentTerminalAction::Cancel
        } else {
            TaskParentTerminalAction::KeepRunning
        },
        completion: if matches!(
            trigger_kind,
            TaskTriggerKind::Interval | TaskTriggerKind::Cron
        ) {
            TaskCompletionBehavior::KeepActiveForRecurring
        } else {
            TaskCompletionBehavior::CompleteOnTerminalRun
        },
    }
}

pub fn default_delivery_policy(
    trigger_kind: TaskTriggerKind,
    attachment: TaskAttachmentMode,
    owner_kind: TaskOwnerKind,
    owner_id: Option<&str>,
    created_by_thread_id: Option<&str>,
) -> TaskDeliveryPolicy {
    let has_thread_target =
        owner_kind == TaskOwnerKind::Thread && owner_id.is_some() || created_by_thread_id.is_some();
    let is_scheduled = matches!(
        trigger_kind,
        TaskTriggerKind::ScheduledAt | TaskTriggerKind::Interval | TaskTriggerKind::Cron
    );
    let is_immediate_detached =
        trigger_kind == TaskTriggerKind::Immediate && attachment == TaskAttachmentMode::Detached;
    TaskDeliveryPolicy {
        mode: if (is_scheduled || is_immediate_detached) && has_thread_target {
            TaskDeliveryMode::Thread
        } else {
            TaskDeliveryMode::None
        },
        thread_target: ((is_scheduled || is_immediate_detached) && has_thread_target)
            .then_some(TaskDeliveryThreadTarget::OriginThread),
        thread_id: ((is_scheduled || is_immediate_detached) && has_thread_target)
            .then(|| {
                created_by_thread_id.map(str::to_owned).or_else(|| {
                    (owner_kind == TaskOwnerKind::Thread)
                        .then(|| owner_id.map(str::to_owned))
                        .flatten()
                })
            })
            .flatten(),
        webhook_url: None,
        include_result: true,
        format: TaskDeliveryFormat::Summary,
    }
}

pub fn default_retry_policy() -> TaskRetryPolicy {
    TaskRetryPolicy {
        max_attempts: 1,
        backoff: TaskRetryBackoffKind::None,
        initial_delay_seconds: None,
        max_delay_seconds: None,
        retry_on: Vec::new(),
    }
}
