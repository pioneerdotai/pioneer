use pioneer_protocol::{
    AgentPresentationSnapshot, TaskAttachmentMode, TaskCompletionBehavior, TaskDeliveryFormat,
    TaskDeliveryMode, TaskDeliveryPolicy, TaskDeliveryThreadTarget, TaskLifecyclePolicy,
    TaskOwnerKind, TaskParentTerminalAction, TaskRetryBackoffKind, TaskRetryPolicy,
    TaskTriggerKind,
};
use std::sync::Arc;

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

/// Immutable Agent authorization ceiling captured together with a resolved
/// Task launch.  The Tasks layer persists this opaque security snapshot; only
/// Gateway derives or revalidates its fingerprint against the role registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskAgentAuthorizationGrantSeed {
    pub role_key: String,
    pub policy_generation: u64,
    pub allowed_actions: Vec<String>,
    pub fingerprint: String,
    pub child_launch_grant: pioneer_protocol::ChildAgentLaunchGrantSet,
}

#[derive(Debug, Clone, Default)]
pub struct TaskCreateContext {
    pub actor_id: Option<String>,
    /// When an already-running agent creates a Task, this immutable snapshot
    /// is copied from its execution-bound action binding. Human/System task
    /// creation leaves it absent.
    pub creator_presentation_snapshot: Option<AgentPresentationSnapshot>,
    /// Server-resolved occurrence destination. These fields are populated
    /// only by the execution-bound Agent action adapter after composite route
    /// authorization; model-facing Task params cannot set them.
    pub execution_destination_thread_id: Option<String>,
    pub execution_route_id: Option<String>,
    pub execution_route_receipt_json: Option<String>,
    pub execution_route_expires_at_millis: Option<i64>,
    /// Optional reverse route used solely for result delivery. The ingress
    /// route never becomes an outgoing bearer grant for the child execution.
    pub delivery_route_id: Option<String>,
    pub delivery_route_receipt_json: Option<String>,
    pub delivery_route_expires_at_millis: Option<i64>,
    /// Immutable lineage of the Agent execution that authored the Task. This
    /// is persisted for audit even when a scheduled occurrence
    /// must allocate a fresh execution/resource root.
    pub creator_work_graph_root_execution_id: Option<String>,
    /// Execution/resource root inherited by an immediate Agent Task. A
    /// scheduled occurrence leaves this absent and materializes its own root
    /// only when that occurrence is admitted.
    pub work_graph_root_execution_id: Option<String>,
    pub launch_selection: Option<pioneer_protocol::AgentLaunchSelection>,
    /// Exact server-resolved catalog facts captured with an agent-authored
    /// create/schedule action. They are persisted in the Task actor contract
    /// so an occurrence never re-resolves a different identity/profile after
    /// configuration changes or restart.
    pub resolved_launch_identity: Option<pioneer_protocol::AgentIdentityProjection>,
    pub resolved_launch_profile: Option<pioneer_protocol::AgentExecutionProfileProjection>,
    pub agent_authorization_grant: Option<TaskAgentAuthorizationGrantSeed>,
    pub conversation_snapshot: Option<TaskRunConversationSnapshotSeed>,
    pub execution_admission: Option<TaskExecutionAdmissionSeed>,
    /// Canonical agent domain action write-set supplied by the execution-bound
    /// Gateway adapter. Task Service never derives or edits this actor-bound
    /// receipt; CRUD commits it with the Task aggregate.
    pub agent_action_commit: Option<pioneer_crud::AgentCommitInput>,
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
    pub agent_action_commit: Option<pioneer_crud::AgentCommitInput>,
}

impl TaskMutationContext {
    pub fn parent_agent(thread_id: impl Into<String>, turn_id: impl Into<String>) -> Self {
        Self {
            actor_id: None,
            thread_id: Some(thread_id.into()),
            turn_id: Some(turn_id.into()),
            execution_admission: None,
            agent_action_commit: None,
        }
    }

    pub fn user(actor_id: impl Into<String>) -> Self {
        Self {
            actor_id: Some(actor_id.into()),
            thread_id: None,
            turn_id: None,
            execution_admission: None,
            agent_action_commit: None,
        }
    }
}

pub type TaskWaitActivityObserver = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone, Default)]
pub struct TaskWaitContext {
    pub actor_id: Option<String>,
    pub task_resource_budget: Option<pioneer_protocol::TaskResourceBudget>,
    /// Called only when durable Task state or a live execution heartbeat
    /// proves that an observed target is still making progress.
    pub confirmed_activity: Option<TaskWaitActivityObserver>,
}

impl std::fmt::Debug for TaskWaitContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskWaitContext")
            .field("actor_id", &self.actor_id)
            .field("task_resource_budget", &self.task_resource_budget)
            .field("confirmed_activity", &self.confirmed_activity.is_some())
            .finish()
    }
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
