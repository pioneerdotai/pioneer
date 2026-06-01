use crate::constants::events;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum TaskValue {
    Null,
    Bool(bool),
    Integer(i64),
    Number(f64),
    String(String),
    List(Vec<TaskValue>),
    Object(BTreeMap<String, TaskValue>),
}

impl Default for TaskValue {
    fn default() -> Self {
        Self::Null
    }
}

impl From<bool> for TaskValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for TaskValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<f64> for TaskValue {
    fn from(value: f64) -> Self {
        Self::Number(value)
    }
}

impl From<String> for TaskValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for TaskValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Draft,
    Scheduled,
    Queued,
    Running,
    Waiting,
    WaitingReview,
    Completed,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskTriggerKind {
    Immediate,
    ScheduledAt,
    Interval,
    Cron,
    Manual,
    External,
    Dependency,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskTriggerStatus {
    Active,
    Paused,
    Exhausted,
    Cancelled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskManualActor {
    Owner,
    WorkspaceMember,
    System,
    Any,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskDependencyTriggerMode {
    AllSucceeded,
    AnySucceeded,
    AllTerminal,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskTriggerCatchUpMode {
    RunOnceForLatestMissed,
    SkipMissed,
    RunAllMissed,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskTriggerCatchUpPolicy {
    pub mode: TaskTriggerCatchUpMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_count: Option<u32>,
}

impl TaskTriggerCatchUpPolicy {
    pub const fn run_once_for_latest_missed() -> Self {
        Self {
            mode: TaskTriggerCatchUpMode::RunOnceForLatestMissed,
            max_count: None,
        }
    }

    pub const fn skip_missed() -> Self {
        Self {
            mode: TaskTriggerCatchUpMode::SkipMissed,
            max_count: None,
        }
    }

    pub const fn run_all_missed(max_count: u32) -> Self {
        Self {
            mode: TaskTriggerCatchUpMode::RunAllMissed,
            max_count: Some(max_count),
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskRescheduleReason {
    Unknown,
    UserRequested,
    TriggerFired,
    MissedFireSkipped,
    RunTerminalStatusRefresh,
    TaskCancelled,
}

impl Default for TaskRescheduleReason {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDependencyTriggerPolicy {
    pub mode: TaskDependencyTriggerMode,
    #[serde(default)]
    pub depends_on_task_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskExternalTriggerFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    #[serde(default)]
    pub fields: BTreeMap<String, TaskValue>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TaskTriggerSpec {
    Immediate,
    ScheduledAt {
        scheduled_at: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timezone: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        catch_up_policy: Option<TaskTriggerCatchUpPolicy>,
    },
    Interval {
        interval_seconds: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        interval_anchor_at: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        catch_up_policy: Option<TaskTriggerCatchUpPolicy>,
    },
    Cron {
        cron_expr: String,
        timezone: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        catch_up_policy: Option<TaskTriggerCatchUpPolicy>,
    },
    Manual {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        allowed_actor: Option<TaskManualActor>,
    },
    External {
        source: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_type: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filter: Option<TaskExternalTriggerFilter>,
    },
    Dependency {
        policy: TaskDependencyTriggerPolicy,
    },
}

impl TaskTriggerSpec {
    pub fn kind(&self) -> TaskTriggerKind {
        match self {
            Self::Immediate => TaskTriggerKind::Immediate,
            Self::ScheduledAt { .. } => TaskTriggerKind::ScheduledAt,
            Self::Interval { .. } => TaskTriggerKind::Interval,
            Self::Cron { .. } => TaskTriggerKind::Cron,
            Self::Manual { .. } => TaskTriggerKind::Manual,
            Self::External { .. } => TaskTriggerKind::External,
            Self::Dependency { .. } => TaskTriggerKind::Dependency,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskExecutorKind {
    Agent,
    Tool,
    Workflow,
    Webhook,
    System,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunStatus {
    Queued,
    Starting,
    Running,
    Waiting,
    WaitingReview,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl TaskRunStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunExecutionStatus {
    Reserved,
    Starting,
    Running,
    WaitingReview,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
}

impl TaskRunExecutionStatus {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut
        )
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunThreadBindingKind {
    PrimaryExecutor,
    Reviewer,
    Recovery,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunTurnKind {
    Initial,
    Revision,
    Recovery,
    Review,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunTurnStatus {
    InProgress,
    CandidateCreated,
    ReviewRecorded,
    Failed,
    Interrupted,
    Cancelled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskResultCandidateStatus {
    PendingReview,
    Accepted,
    Rejected,
    Superseded,
    ExtractionFailed,
    Cancelled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskResultReviewerKind {
    RuntimeAuto,
    ParentAgent,
    ReviewAgent,
    User,
    System,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskResultReviewEventKind {
    Advisory,
    Decision,
    Override,
    SystemAuto,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskResultReviewDecision {
    Accept,
    RequestChanges,
    Reject,
    Abstain,
    Cancel,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskAgentReviewMode {
    None,
    ParentAgent,
    ParentAgentWithReviewers,
    UserApproval,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskResultReviewResolutionStrategy {
    ParentFinal,
    UserFinal,
    RequireAllRequiredReviewersThenParent,
    QuorumThenParent,
    AnyRequiredReviewerCanRequestChanges,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskResultReviewerSpec {
    pub reviewer_kind: TaskResultReviewerKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_nickname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_role: Option<String>,
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<f64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentReviewPolicy {
    pub mode: TaskAgentReviewMode,
    pub max_revision_rounds: u32,
    pub require_explicit_acceptance: bool,
    #[serde(default)]
    pub reviewers: Vec<TaskResultReviewerSpec>,
    pub resolution_strategy: TaskResultReviewResolutionStrategy,
}

impl TaskAgentReviewPolicy {
    pub fn parent_agent_default(max_revision_rounds: u32) -> Self {
        Self {
            mode: TaskAgentReviewMode::ParentAgent,
            max_revision_rounds,
            require_explicit_acceptance: true,
            reviewers: Vec::new(),
            resolution_strategy: TaskResultReviewResolutionStrategy::ParentFinal,
        }
    }

    pub const fn is_enabled(&self) -> bool {
        !matches!(self.mode, TaskAgentReviewMode::None)
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskOwnerKind {
    User,
    Thread,
    Workspace,
    System,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskAttachmentMode {
    Attached,
    Detached,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskParentTerminalAction {
    Cancel,
    Detach,
    KeepRunning,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskCompletionBehavior {
    CompleteOnTerminalRun,
    KeepActiveForRecurring,
    Manual,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskLifecyclePolicy {
    pub attachment: TaskAttachmentMode,
    pub on_parent_cancel: TaskParentTerminalAction,
    pub on_parent_failure: TaskParentTerminalAction,
    pub completion: TaskCompletionBehavior,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskDeliveryMode {
    None,
    OwnerThread,
    Thread,
    UserNotification,
    Webhook,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskDeliveryFormat {
    Summary,
    FullResult,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDeliveryPolicy {
    pub mode: TaskDeliveryMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook_url: Option<String>,
    pub include_result: bool,
    pub format: TaskDeliveryFormat,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskDeliveryStatus {
    Pending,
    Delivering,
    Delivered,
    Failed,
    Cancelled,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskDeliveryAttemptStatus {
    Started,
    Delivered,
    Failed,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDelivery {
    pub id: String,
    pub workspace_id: String,
    pub task_id: String,
    pub run_id: String,
    pub delivery_key: String,
    pub mode: TaskDeliveryMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook_url_fingerprint: Option<String>,
    pub status: TaskDeliveryStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<i64>,
    pub attempt_count: u32,
    pub max_attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_snapshot: Option<TaskResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_snapshot: Option<TaskError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_notification_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDeliveryAttempt {
    pub id: String,
    pub delivery_id: String,
    pub attempt_number: u32,
    pub status: TaskDeliveryAttemptStatus,
    pub started_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_fingerprint: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskRetryBackoffKind {
    None,
    Fixed,
    Exponential,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskErrorClass {
    Cancelled,
    Timeout,
    Provider,
    Tool,
    Validation,
    Dependency,
    Policy,
    Internal,
    Unknown,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRetryPolicy {
    pub max_attempts: u32,
    pub backoff: TaskRetryBackoffKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_delay_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_delay_seconds: Option<i64>,
    #[serde(default)]
    pub retry_on: Vec<TaskErrorClass>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskTimeoutPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_timeout_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_timeout_seconds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat_timeout_seconds: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskConcurrencyConflictPolicy {
    Queue,
    Reject,
    CancelExisting,
    Allow,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskConcurrencyPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub max_parallel_runs: u32,
    pub on_conflict: TaskConcurrencyConflictPolicy,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskCancelScope {
    TaskOnly,
    AttachedSubtree,
    FullSubtree,
}

impl Default for TaskCancelScope {
    fn default() -> Self {
        Self::AttachedSubtree
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskWriteLockScopeKind {
    Workspace,
    Path,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskWriteLockStatus {
    Pending,
    Acquired,
    Released,
    Expired,
    Cancelled,
    Blocked,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskWriteLock {
    pub id: String,
    pub workspace_id: String,
    pub task_id: String,
    pub run_id: String,
    pub scope_kind: TaskWriteLockScopeKind,
    pub scope_path: String,
    pub status: TaskWriteLockStatus,
    pub acquired_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub released_at: Option<i64>,
    pub conflict_policy: TaskConcurrencyConflictPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskWriteLockConflict {
    pub lock_id: String,
    pub task_id: String,
    pub run_id: String,
    pub scope_kind: TaskWriteLockScopeKind,
    pub scope_path: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskMetadata {
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<TaskValue>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskArtifact {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<TaskValue>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<TaskValue>,
    #[serde(default)]
    pub artifacts: Vec<TaskArtifact>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_by_run_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskError {
    pub code: String,
    pub message: String,
    pub class: TaskErrorClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<TaskValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failed_run_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub id: String,
    pub workspace_id: String,
    pub owner_kind: TaskOwnerKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    pub executor_kind: TaskExecutorKind,
    pub status: TaskStatus,
    pub title: String,
    pub goal: String,
    pub priority: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_policy: Option<TaskLifecyclePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_policy: Option<TaskDeliveryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<TaskRetryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_policy: Option<TaskTimeoutPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_policy: Option<TaskConcurrencyPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<TaskMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<TaskResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<TaskError>,
    pub revision: i64,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskTrigger {
    pub id: String,
    pub task_id: String,
    pub status: TaskTriggerStatus,
    pub spec: TaskTriggerSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_fire_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fire_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl TaskTrigger {
    pub fn kind(&self) -> TaskTriggerKind {
        self.spec.kind()
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentInputVariable {
    pub name: String,
    pub value: TaskValue,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskAgentInputAttachmentKind {
    File,
    Artifact,
    Url,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentInputAttachment {
    pub kind: TaskAgentInputAttachmentKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskAgentInputReferenceKind {
    Thread,
    Turn,
    Task,
    TaskRun,
    Artifact,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentInputReference {
    pub kind: TaskAgentInputReferenceKind,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default)]
    pub variables: Vec<TaskAgentInputVariable>,
    #[serde(default)]
    pub attachments: Vec<TaskAgentInputAttachment>,
    #[serde(default)]
    pub references: Vec<TaskAgentInputReference>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentPrompt {
    pub goal: String,
    #[serde(default)]
    pub instructions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<TaskAgentInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_instructions: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskAgentContextMode {
    InheritParent,
    LastNTurns,
    SummaryOnly,
    Empty,
    Custom,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default)]
    pub variables: Vec<TaskAgentInputVariable>,
    #[serde(default)]
    pub attachments: Vec<TaskAgentInputAttachment>,
    #[serde(default)]
    pub references: Vec<TaskAgentInputReference>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentContextPolicy {
    pub mode: TaskAgentContextMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turns: Option<u32>,
    pub include_parent_summary: bool,
    pub include_artifacts: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_context: Option<TaskAgentContext>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskAgentWriteMode {
    ReadOnly,
    WorkspaceWrite,
    ScopedWrite,
    FullAccess,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentToolPolicy {
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub denied_tools: Vec<String>,
    pub write_mode: TaskAgentWriteMode,
    #[serde(default)]
    pub allowed_paths: Vec<String>,
    pub network_access: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskAgentResultFormat {
    Text,
    Markdown,
    Json,
    Artifact,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub schema: TaskValue,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentResultContract {
    pub format: TaskAgentResultFormat,
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<TaskSchema>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRun {
    pub id: String,
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    pub run_group_id: String,
    pub attempt_number: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_of_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_at: Option<i64>,
    pub run_number: i64,
    pub status: TaskRunStatus,
    pub executor_kind: TaskExecutorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locked_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_expires_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<TaskResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<TaskError>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRunExecution {
    pub id: String,
    pub task_id: String,
    pub task_run_id: String,
    pub executor_kind: TaskExecutorKind,
    pub status: TaskRunExecutionStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_until: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<TaskResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<TaskError>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRunThreadBinding {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    pub thread_id: String,
    pub binding_kind: TaskRunThreadBindingKind,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskThreadLineage {
    pub child_thread_id: String,
    pub parent_thread_id: String,
    pub root_thread_id: String,
    pub depth: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_turn_id: Option<String>,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRunTurn {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    pub thread_id: String,
    pub turn_id: String,
    pub kind: TaskRunTurnKind,
    pub round: u32,
    pub sequence: u32,
    pub status: TaskRunTurnStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviews_candidate_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_by_candidate_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_by_review_event_id: Option<String>,
    pub created_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskResultCandidate {
    pub id: String,
    pub task_id: String,
    pub run_id: String,
    pub task_run_turn_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub round: u32,
    pub status: TaskResultCandidateStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<TaskResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extraction_error: Option<TaskError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_review_event_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskResultReviewEvent {
    pub id: String,
    pub candidate_id: String,
    pub task_id: String,
    pub run_id: String,
    pub task_run_turn_id: String,
    pub reviewer_kind: TaskResultReviewerKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_agent_spec_id: Option<String>,
    pub event_kind: TaskResultReviewEventKind,
    pub decision: TaskResultReviewDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<TaskValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes_review_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_task_run_turn_id: Option<String>,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentSpec {
    pub id: String,
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_nickname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    pub prompt: TaskAgentPrompt,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_policy: Option<TaskAgentContextPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_policy: Option<TaskAgentToolPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_contract: Option<TaskAgentResultContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_policy: Option<TaskAgentReviewPolicy>,
    pub depth: i64,
    pub max_depth: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDependencyCondition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<TaskValue>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDependency {
    pub id: String,
    pub task_id: String,
    pub depends_on_task_id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<TaskDependencyCondition>,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskEvent {
    pub id: String,
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub sequence: i64,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    pub payload: TaskEventPayload,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadLineage {
    pub child_thread_id: String,
    pub child_turn_id: String,
    pub parent_thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_turn_id: Option<String>,
    pub task_id: String,
    pub task_run_id: String,
    pub root_thread_id: String,
    pub depth: i64,
    pub created_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum TaskEventPayload {
    TaskCreated {
        task: Task,
    },
    TriggerCreated {
        trigger: TaskTrigger,
    },
    DependencyCreated {
        dependency: TaskDependency,
    },
    AgentSpecCreated {
        agent_spec: TaskAgentSpec,
    },
    TaskScheduled {
        task_id: String,
        trigger_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        next_fire_at: Option<i64>,
    },
    TaskQueued {
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
    },
    RunCreated {
        run: TaskRun,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent_spec: Option<TaskAgentSpec>,
    },
    RunStarted {
        task_id: String,
        run_id: String,
        started_at: i64,
    },
    Progress {
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<TaskProgressDetails>,
    },
    RunCompleted {
        task_id: String,
        run_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<TaskResult>,
        completed_at: i64,
    },
    RunFailed {
        task_id: String,
        run_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<TaskError>,
        completed_at: i64,
    },
    RunRetryScheduled {
        task_id: String,
        failed_run_id: String,
        retry_run: TaskRun,
        next_attempt_at: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<TaskError>,
    },
    RunRetryExhausted {
        task_id: String,
        run_group_id: String,
        final_run_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<TaskError>,
        exhausted_at: i64,
    },
    RunCancelled {
        task_id: String,
        run_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        cancelled_at: i64,
    },
    TaskCompleted {
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        result: Option<TaskResult>,
        completed_at: i64,
    },
    TaskFailed {
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<TaskError>,
        completed_at: i64,
    },
    TaskCancelled {
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        completed_at: i64,
    },
    TaskDetached {
        task: Task,
        detached_at: i64,
    },
    TaskUpdated {
        task: Task,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trigger: Option<TaskTrigger>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent_spec: Option<TaskAgentSpec>,
        #[serde(default)]
        changed_fields: Vec<String>,
        updated_at: i64,
    },
    TaskRescheduled {
        task_id: String,
        trigger: TaskTrigger,
        rescheduled_at: i64,
        #[serde(default)]
        reason: TaskRescheduleReason,
    },
    TaskPaused {
        task: Task,
        #[serde(default)]
        triggers: Vec<TaskTrigger>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        paused_at: i64,
    },
    TaskResumed {
        task: Task,
        #[serde(default)]
        triggers: Vec<TaskTrigger>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        resumed_at: i64,
    },
    TaskRecovered {
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
        message: String,
        recovered_at: i64,
    },
    ChildThreadLinked {
        lineage: ThreadLineage,
    },
    TaskThreadLineageCreated {
        task_id: String,
        run_id: String,
        lineage: TaskThreadLineage,
    },
    TaskRunThreadBindingCreated {
        binding: TaskRunThreadBinding,
    },
    TaskRunTurnStarted {
        task_run_turn: TaskRunTurn,
    },
    TaskRunTurnCompleted {
        task_run_turn: TaskRunTurn,
    },
    TaskRunTurnFailed {
        task_run_turn: TaskRunTurn,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<TaskError>,
    },
    TaskResultCandidateCreated {
        candidate: TaskResultCandidate,
    },
    TaskResultReviewEventRecorded {
        review_event: TaskResultReviewEvent,
    },
    TaskResultCandidateAccepted {
        candidate: TaskResultCandidate,
        review_event_id: String,
    },
    TaskResultCandidateRejected {
        candidate: TaskResultCandidate,
        review_event_id: String,
    },
    TaskResultCandidateCancelled {
        candidate: TaskResultCandidate,
        review_event_id: String,
    },
    TaskRevisionRequested {
        task_id: String,
        run_id: String,
        previous_candidate_id: String,
        requested_by_review_event_id: String,
        task_run_turn_id: String,
        thread_id: String,
        turn_id: String,
        round: u32,
        feedback: String,
        requested_at: i64,
    },
    TaskRunEnteredReview {
        task_id: String,
        run_id: String,
        candidate_id: String,
        entered_at: i64,
    },
    DepthLimitExceeded {
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        run_id: Option<String>,
        depth: i64,
        max_depth: i64,
    },
    DeliveryQueued {
        delivery: TaskDelivery,
    },
    DeliveryStarted {
        delivery: TaskDelivery,
        attempt: TaskDeliveryAttempt,
    },
    DeliveryDelivered {
        delivery: TaskDelivery,
        attempt: TaskDeliveryAttempt,
    },
    DeliveryFailed {
        delivery: TaskDelivery,
        attempt: TaskDeliveryAttempt,
    },
    DeliveryCancelled {
        delivery: TaskDelivery,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    WriteLockAcquired {
        lock: TaskWriteLock,
    },
    WriteLockExtended {
        lock: TaskWriteLock,
        extended_at: i64,
    },
    WriteLockReleased {
        lock: TaskWriteLock,
        released_at: i64,
    },
    WriteLockBlocked {
        task_id: String,
        run_id: String,
        #[serde(default)]
        conflicts: Vec<TaskWriteLockConflict>,
        blocked_at: i64,
    },
    WriteLockExpired {
        lock: TaskWriteLock,
        expired_at: i64,
    },
}

impl TaskEventPayload {
    pub fn task_id(&self) -> &str {
        match self {
            Self::TaskCreated { task } => task.id.as_str(),
            Self::TriggerCreated { trigger } => trigger.task_id.as_str(),
            Self::DependencyCreated { dependency } => dependency.task_id.as_str(),
            Self::AgentSpecCreated { agent_spec } => agent_spec.task_id.as_str(),
            Self::TaskUpdated { task, .. } => task.id.as_str(),
            Self::TaskScheduled { task_id, .. }
            | Self::TaskQueued { task_id, .. }
            | Self::RunStarted { task_id, .. }
            | Self::Progress { task_id, .. }
            | Self::RunCompleted { task_id, .. }
            | Self::RunFailed { task_id, .. }
            | Self::RunRetryScheduled { task_id, .. }
            | Self::RunRetryExhausted { task_id, .. }
            | Self::RunCancelled { task_id, .. }
            | Self::TaskCompleted { task_id, .. }
            | Self::TaskFailed { task_id, .. }
            | Self::TaskCancelled { task_id, .. }
            | Self::TaskRescheduled { task_id, .. }
            | Self::TaskRecovered { task_id, .. }
            | Self::DepthLimitExceeded { task_id, .. }
            | Self::WriteLockBlocked { task_id, .. } => task_id.as_str(),
            Self::TaskPaused { task, .. } | Self::TaskResumed { task, .. } => task.id.as_str(),
            Self::DeliveryQueued { delivery }
            | Self::DeliveryStarted { delivery, .. }
            | Self::DeliveryDelivered { delivery, .. }
            | Self::DeliveryFailed { delivery, .. }
            | Self::DeliveryCancelled { delivery, .. } => delivery.task_id.as_str(),
            Self::WriteLockAcquired { lock }
            | Self::WriteLockExtended { lock, .. }
            | Self::WriteLockReleased { lock, .. }
            | Self::WriteLockExpired { lock, .. } => lock.task_id.as_str(),
            Self::RunCreated { run, .. } => run.task_id.as_str(),
            Self::TaskDetached { task, .. } => task.id.as_str(),
            Self::ChildThreadLinked { lineage } => lineage.task_id.as_str(),
            Self::TaskThreadLineageCreated { task_id, .. } => task_id.as_str(),
            Self::TaskRunThreadBindingCreated { binding } => binding.task_id.as_str(),
            Self::TaskRunTurnStarted { task_run_turn }
            | Self::TaskRunTurnCompleted { task_run_turn }
            | Self::TaskRunTurnFailed { task_run_turn, .. } => task_run_turn.task_id.as_str(),
            Self::TaskResultCandidateCreated { candidate }
            | Self::TaskResultCandidateAccepted { candidate, .. }
            | Self::TaskResultCandidateRejected { candidate, .. }
            | Self::TaskResultCandidateCancelled { candidate, .. } => candidate.task_id.as_str(),
            Self::TaskResultReviewEventRecorded { review_event } => review_event.task_id.as_str(),
            Self::TaskRevisionRequested { task_id, .. }
            | Self::TaskRunEnteredReview { task_id, .. } => task_id.as_str(),
        }
    }

    pub fn run_id(&self) -> Option<&str> {
        match self {
            Self::TaskCreated { .. }
            | Self::TriggerCreated { .. }
            | Self::DependencyCreated { .. }
            | Self::AgentSpecCreated { .. }
            | Self::TaskUpdated { .. }
            | Self::TaskScheduled { .. }
            | Self::TaskRescheduled { .. }
            | Self::TaskPaused { .. }
            | Self::TaskResumed { .. } => None,
            Self::TaskQueued { run_id, .. }
            | Self::Progress { run_id, .. }
            | Self::TaskRecovered { run_id, .. }
            | Self::DepthLimitExceeded { run_id, .. } => run_id.as_deref(),
            Self::RunCreated { run, .. } => Some(run.id.as_str()),
            Self::RunStarted { run_id, .. }
            | Self::RunCompleted { run_id, .. }
            | Self::RunFailed { run_id, .. }
            | Self::RunCancelled { run_id, .. } => Some(run_id.as_str()),
            Self::RunRetryScheduled { retry_run, .. } => Some(retry_run.id.as_str()),
            Self::RunRetryExhausted { final_run_id, .. } => Some(final_run_id.as_str()),
            Self::TaskCompleted { .. }
            | Self::TaskFailed { .. }
            | Self::TaskCancelled { .. }
            | Self::TaskDetached { .. } => None,
            Self::ChildThreadLinked { lineage } => Some(lineage.task_run_id.as_str()),
            Self::TaskThreadLineageCreated { run_id, .. } => Some(run_id.as_str()),
            Self::TaskRunThreadBindingCreated { binding } => Some(binding.run_id.as_str()),
            Self::TaskRunTurnStarted { task_run_turn }
            | Self::TaskRunTurnCompleted { task_run_turn }
            | Self::TaskRunTurnFailed { task_run_turn, .. } => Some(task_run_turn.run_id.as_str()),
            Self::TaskResultCandidateCreated { candidate }
            | Self::TaskResultCandidateAccepted { candidate, .. }
            | Self::TaskResultCandidateRejected { candidate, .. }
            | Self::TaskResultCandidateCancelled { candidate, .. } => {
                Some(candidate.run_id.as_str())
            }
            Self::TaskResultReviewEventRecorded { review_event } => {
                Some(review_event.run_id.as_str())
            }
            Self::TaskRevisionRequested { run_id, .. }
            | Self::TaskRunEnteredReview { run_id, .. } => Some(run_id.as_str()),
            Self::DeliveryQueued { delivery }
            | Self::DeliveryStarted { delivery, .. }
            | Self::DeliveryDelivered { delivery, .. }
            | Self::DeliveryFailed { delivery, .. }
            | Self::DeliveryCancelled { delivery, .. } => Some(delivery.run_id.as_str()),
            Self::WriteLockAcquired { lock }
            | Self::WriteLockExtended { lock, .. }
            | Self::WriteLockReleased { lock, .. }
            | Self::WriteLockExpired { lock, .. } => Some(lock.run_id.as_str()),
            Self::WriteLockBlocked { run_id, .. } => Some(run_id.as_str()),
        }
    }

    pub fn thread_id(&self) -> Option<&str> {
        match self {
            Self::ChildThreadLinked { lineage } => Some(lineage.child_thread_id.as_str()),
            Self::TaskThreadLineageCreated { lineage, .. } => {
                Some(lineage.child_thread_id.as_str())
            }
            Self::TaskRunThreadBindingCreated { binding } => Some(binding.thread_id.as_str()),
            Self::TaskRunTurnStarted { task_run_turn }
            | Self::TaskRunTurnCompleted { task_run_turn }
            | Self::TaskRunTurnFailed { task_run_turn, .. } => {
                Some(task_run_turn.thread_id.as_str())
            }
            Self::TaskResultCandidateCreated { candidate }
            | Self::TaskResultCandidateAccepted { candidate, .. }
            | Self::TaskResultCandidateRejected { candidate, .. }
            | Self::TaskResultCandidateCancelled { candidate, .. } => {
                Some(candidate.thread_id.as_str())
            }
            Self::TaskResultReviewEventRecorded { review_event } => {
                review_event.reviewer_thread_id.as_deref()
            }
            Self::TaskRevisionRequested { thread_id, .. } => Some(thread_id.as_str()),
            Self::DeliveryQueued { delivery }
            | Self::DeliveryStarted { delivery, .. }
            | Self::DeliveryDelivered { delivery, .. }
            | Self::DeliveryFailed { delivery, .. }
            | Self::DeliveryCancelled { delivery, .. } => delivery.target_thread_id.as_deref(),
            _ => None,
        }
    }

    pub fn turn_id(&self) -> Option<&str> {
        match self {
            Self::ChildThreadLinked { lineage } => Some(lineage.child_turn_id.as_str()),
            Self::TaskRunTurnStarted { task_run_turn }
            | Self::TaskRunTurnCompleted { task_run_turn }
            | Self::TaskRunTurnFailed { task_run_turn, .. } => Some(task_run_turn.turn_id.as_str()),
            Self::TaskResultCandidateCreated { candidate }
            | Self::TaskResultCandidateAccepted { candidate, .. }
            | Self::TaskResultCandidateRejected { candidate, .. }
            | Self::TaskResultCandidateCancelled { candidate, .. } => {
                Some(candidate.turn_id.as_str())
            }
            Self::TaskResultReviewEventRecorded { review_event } => {
                review_event.reviewer_turn_id.as_deref()
            }
            Self::TaskRevisionRequested { turn_id, .. } => Some(turn_id.as_str()),
            _ => None,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            Self::TaskCreated { .. } => events::TASK_CREATED,
            Self::TriggerCreated { trigger } => match trigger.kind() {
                TaskTriggerKind::Immediate => events::TASK_QUEUED,
                _ => events::TASK_SCHEDULED,
            },
            Self::DependencyCreated { .. } => events::TASK_TREE_CHANGED,
            Self::AgentSpecCreated { .. } => events::TASK_TREE_CHANGED,
            Self::TaskScheduled { .. } => events::TASK_SCHEDULED,
            Self::TaskQueued { .. } => events::TASK_QUEUED,
            Self::RunCreated { .. } => events::TASK_RUN_CREATED,
            Self::RunStarted { .. } => events::TASK_RUN_STARTED,
            Self::Progress { .. } => events::TASK_PROGRESS,
            Self::RunCompleted { .. } => events::TASK_RUN_COMPLETED,
            Self::RunFailed { .. } => events::TASK_RUN_FAILED,
            Self::RunRetryScheduled { .. } => events::TASK_RUN_RETRY_SCHEDULED,
            Self::RunRetryExhausted { .. } => events::TASK_RUN_RETRY_EXHAUSTED,
            Self::RunCancelled { .. } => events::TASK_RUN_CANCELLED,
            Self::TaskCompleted { .. } => events::TASK_COMPLETED,
            Self::TaskFailed { .. } => events::TASK_FAILED,
            Self::TaskCancelled { .. } => events::TASK_CANCELLED,
            Self::TaskDetached { .. } => events::TASK_DETACHED,
            Self::TaskUpdated { .. } => events::TASK_UPDATED,
            Self::TaskRescheduled { .. } => events::TASK_RESCHEDULED,
            Self::TaskPaused { .. } => events::TASK_PAUSED,
            Self::TaskResumed { .. } => events::TASK_RESUMED,
            Self::TaskRecovered { .. } => events::TASK_RECOVERED,
            Self::ChildThreadLinked { .. } => events::TASK_TREE_CHANGED,
            Self::TaskThreadLineageCreated { .. } => events::TASK_TREE_CHANGED,
            Self::TaskRunThreadBindingCreated { .. } => events::TASK_RUN_THREAD_BINDING_CREATED,
            Self::TaskRunTurnStarted { .. } => events::TASK_RUN_TURN_STARTED,
            Self::TaskRunTurnCompleted { .. } => events::TASK_RUN_TURN_COMPLETED,
            Self::TaskRunTurnFailed { .. } => events::TASK_RUN_TURN_FAILED,
            Self::TaskResultCandidateCreated { .. } => events::TASK_RESULT_CANDIDATE_CREATED,
            Self::TaskResultReviewEventRecorded { .. } => events::TASK_RESULT_REVIEW_EVENT_RECORDED,
            Self::TaskResultCandidateAccepted { .. } => events::TASK_RESULT_CANDIDATE_ACCEPTED,
            Self::TaskResultCandidateRejected { .. } => events::TASK_RESULT_CANDIDATE_REJECTED,
            Self::TaskResultCandidateCancelled { .. } => events::TASK_RESULT_CANDIDATE_CANCELLED,
            Self::TaskRevisionRequested { .. } => events::TASK_REVISION_REQUESTED,
            Self::TaskRunEnteredReview { .. } => events::TASK_RUN_ENTERED_REVIEW,
            Self::DepthLimitExceeded { .. } => events::TASK_FAILED,
            Self::DeliveryQueued { .. } => events::TASK_DELIVERY_QUEUED,
            Self::DeliveryStarted { .. } => events::TASK_DELIVERY_STARTED,
            Self::DeliveryDelivered { .. } => events::TASK_DELIVERY_DELIVERED,
            Self::DeliveryFailed { .. } => events::TASK_DELIVERY_FAILED,
            Self::DeliveryCancelled { .. } => events::TASK_DELIVERY_CANCELLED,
            Self::WriteLockAcquired { .. } => events::TASK_WRITE_LOCK_ACQUIRED,
            Self::WriteLockExtended { .. } => events::TASK_WRITE_LOCK_EXTENDED,
            Self::WriteLockReleased { .. } => events::TASK_WRITE_LOCK_RELEASED,
            Self::WriteLockBlocked { .. } => events::TASK_WRITE_LOCK_BLOCKED,
            Self::WriteLockExpired { .. } => events::TASK_WRITE_LOCK_EXPIRED,
        }
    }

    pub fn idempotency_key(&self) -> Option<String> {
        match self {
            Self::TaskCreated { task } => Some(format!("task:{}:created", task.id)),
            Self::TriggerCreated { trigger } => Some(format!("trigger:{}:created", trigger.id)),
            Self::DependencyCreated { dependency } => {
                Some(format!("dependency:{}:created", dependency.id))
            }
            Self::AgentSpecCreated { agent_spec } => {
                Some(format!("agent_spec:{}:created", agent_spec.id))
            }
            Self::TaskScheduled {
                trigger_id,
                next_fire_at,
                ..
            } => Some(format!(
                "trigger:{trigger_id}:scheduled:{}",
                next_fire_at
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_owned())
            )),
            Self::TaskQueued { task_id, run_id } => run_id
                .as_deref()
                .map(|run_id| format!("task:{task_id}:run:{run_id}:queued")),
            Self::RunCreated { run, .. } => Some(format!("run:{}:created", run.id)),
            Self::RunStarted { run_id, .. } => Some(format!("run:{run_id}:started")),
            Self::RunCompleted { run_id, .. }
            | Self::RunFailed { run_id, .. }
            | Self::RunCancelled { run_id, .. } => Some(format!("run:{run_id}:terminal")),
            Self::RunRetryScheduled { retry_run, .. } => {
                Some(format!("run:{}:retry_created", retry_run.id))
            }
            Self::RunRetryExhausted {
                run_group_id,
                final_run_id,
                ..
            } => Some(format!(
                "run_group:{run_group_id}:retry_exhausted:{final_run_id}"
            )),
            Self::TaskCompleted { task_id, .. }
            | Self::TaskFailed { task_id, .. }
            | Self::TaskCancelled { task_id, .. } => Some(format!("task:{task_id}:terminal")),
            Self::TaskDetached { task, detached_at } => Some(format!(
                "task:{}:detached:{}:{detached_at}",
                task.id, task.revision
            )),
            Self::TaskUpdated { .. } | Self::TaskPaused { .. } | Self::TaskResumed { .. } => None,
            Self::TaskRescheduled {
                trigger,
                reason,
                rescheduled_at,
                ..
            } => Some(format!(
                "trigger:{}:rescheduled:{reason:?}:{:?}:{}:{}",
                trigger.id,
                trigger.status,
                trigger
                    .next_fire_at
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_owned()),
                trigger
                    .last_fire_at
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| rescheduled_at.to_string())
            )),
            Self::TaskRecovered { .. } => None,
            Self::ChildThreadLinked { lineage } => {
                Some(format!("run:{}:child_thread_linked", lineage.task_run_id))
            }
            Self::TaskThreadLineageCreated {
                run_id, lineage, ..
            } => Some(format!(
                "run:{run_id}:thread:{}:lineage_created",
                lineage.child_thread_id
            )),
            Self::TaskRunThreadBindingCreated { binding } => Some(format!(
                "run:{}:thread:{}:binding:{}",
                binding.run_id,
                binding.thread_id,
                serde_json::to_value(binding.binding_kind)
                    .ok()
                    .and_then(|value| value.as_str().map(str::to_owned))
                    .unwrap_or_else(|| "unknown".to_owned())
            )),
            Self::TaskRunTurnStarted { task_run_turn } => Some(format!(
                "run:{}:turn:{}:started",
                task_run_turn.run_id, task_run_turn.turn_id
            )),
            Self::TaskRunTurnCompleted { task_run_turn } => Some(format!(
                "run:{}:turn:{}:completed",
                task_run_turn.run_id, task_run_turn.turn_id
            )),
            Self::TaskRunTurnFailed { task_run_turn, .. } => Some(format!(
                "run:{}:turn:{}:failed",
                task_run_turn.run_id, task_run_turn.turn_id
            )),
            Self::TaskResultCandidateCreated { candidate } => Some(format!(
                "run:{}:candidate:{}:created",
                candidate.run_id, candidate.turn_id
            )),
            Self::TaskResultReviewEventRecorded { review_event } => Some(format!(
                "run:{}:candidate:{}:review:{}",
                review_event.run_id, review_event.candidate_id, review_event.id
            )),
            Self::TaskResultCandidateAccepted {
                candidate,
                review_event_id,
            } => Some(format!(
                "run:{}:candidate:{}:accepted:{review_event_id}",
                candidate.run_id, candidate.id
            )),
            Self::TaskResultCandidateRejected {
                candidate,
                review_event_id,
            } => Some(format!(
                "run:{}:candidate:{}:rejected:{review_event_id}",
                candidate.run_id, candidate.id
            )),
            Self::TaskResultCandidateCancelled {
                candidate,
                review_event_id,
            } => Some(format!(
                "run:{}:candidate:{}:cancelled:{review_event_id}",
                candidate.run_id, candidate.id
            )),
            Self::TaskRevisionRequested {
                run_id, turn_id, ..
            } => Some(format!("run:{run_id}:revision:{turn_id}:requested")),
            Self::TaskRunEnteredReview {
                run_id,
                candidate_id,
                ..
            } => Some(format!(
                "run:{run_id}:candidate:{candidate_id}:entered_review"
            )),
            Self::DepthLimitExceeded {
                task_id,
                run_id,
                depth,
                max_depth,
            } => Some(format!(
                "task:{task_id}:run:{}:depth_limit_exceeded:{depth}:{max_depth}",
                run_id.as_deref().unwrap_or("none")
            )),
            Self::DeliveryQueued { delivery } => Some(format!("delivery:{}:queued", delivery.id)),
            Self::DeliveryStarted { delivery, attempt } => Some(format!(
                "delivery:{}:attempt:{}:started",
                delivery.id, attempt.id
            )),
            Self::DeliveryDelivered { delivery, .. } => {
                Some(format!("delivery:{}:delivered", delivery.id))
            }
            Self::DeliveryFailed { delivery, attempt } => Some(format!(
                "delivery:{}:attempt:{}:failed",
                delivery.id, attempt.id
            )),
            Self::DeliveryCancelled { delivery, .. } => {
                Some(format!("delivery:{}:cancelled", delivery.id))
            }
            Self::WriteLockAcquired { lock } => Some(format!("write_lock:{}:acquired", lock.id)),
            Self::WriteLockExtended { lock, extended_at } => Some(format!(
                "write_lock:{}:extended:{extended_at}:{}",
                lock.id,
                lock.expires_at
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "none".to_owned())
            )),
            Self::WriteLockReleased { lock, released_at } => {
                Some(format!("write_lock:{}:released:{released_at}", lock.id))
            }
            Self::WriteLockBlocked { .. } => None,
            Self::WriteLockExpired { lock, expired_at } => {
                Some(format!("write_lock:{}:expired:{expired_at}", lock.id))
            }
            Self::Progress { .. } => None,
        }
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskTree {
    pub task: Task,
    #[serde(default)]
    pub triggers: Vec<TaskTrigger>,
    #[serde(default)]
    pub runs: Vec<TaskRun>,
    #[serde(default)]
    pub agent_specs: Vec<TaskAgentSpec>,
    #[serde(default)]
    pub dependencies: Vec<TaskDependency>,
    #[serde(default)]
    pub write_locks: Vec<TaskWriteLock>,
    #[serde(default)]
    pub children: Vec<TaskTree>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskTriggerInput {
    pub spec: TaskTriggerSpec,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgentSpecInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_nickname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    pub prompt: TaskAgentPrompt,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_policy: Option<TaskAgentContextPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_policy: Option<TaskAgentToolPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_contract: Option<TaskAgentResultContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_policy: Option<TaskAgentReviewPolicy>,
    pub depth: i64,
    pub max_depth: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskCreateParams {
    pub workspace_id: String,
    pub owner_kind: TaskOwnerKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    pub executor_kind: TaskExecutorKind,
    pub title: String,
    pub goal: String,
    #[serde(default)]
    pub priority: i64,
    pub trigger: TaskTriggerInput,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_spec: Option<TaskAgentSpecInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_policy: Option<TaskLifecyclePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_policy: Option<TaskDeliveryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<TaskRetryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_policy: Option<TaskTimeoutPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_policy: Option<TaskConcurrencyPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<TaskMetadata>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskCreateResponse {
    pub task: Task,
    pub trigger: TaskTrigger,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<TaskRun>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_spec: Option<TaskAgentSpec>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskUpdateParams {
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger: Option<TaskTriggerInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_nickname: Option<String>,
    #[serde(default)]
    pub instructions: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<TaskAgentInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_policy: Option<TaskAgentContextPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_policy: Option<TaskAgentToolPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_contract: Option<TaskAgentResultContract>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_policy: Option<TaskLifecyclePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_policy: Option<TaskDeliveryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<TaskRetryPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_policy: Option<TaskTimeoutPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_policy: Option<TaskConcurrencyPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<TaskMetadata>,
    #[serde(default)]
    pub clear_agent_role: bool,
    #[serde(default)]
    pub clear_agent_nickname: bool,
    #[serde(default)]
    pub clear_input: bool,
    #[serde(default)]
    pub clear_output_instructions: bool,
    #[serde(default)]
    pub clear_context_policy: bool,
    #[serde(default)]
    pub clear_tool_policy: bool,
    #[serde(default)]
    pub clear_result_contract: bool,
    #[serde(default)]
    pub clear_timeout_policy: bool,
    #[serde(default)]
    pub clear_concurrency_policy: bool,
    #[serde(default)]
    pub clear_metadata: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskUpdateResponse {
    pub task: Task,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger: Option<TaskTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_spec: Option<TaskAgentSpec>,
    #[serde(default)]
    pub changed_fields: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskGetParams {
    pub task_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskGetResponse {
    pub task: Task,
    #[serde(default)]
    pub triggers: Vec<TaskTrigger>,
    #[serde(default)]
    pub runs: Vec<TaskRun>,
    #[serde(default)]
    pub agent_specs: Vec<TaskAgentSpec>,
    #[serde(default)]
    pub dependencies: Vec<TaskDependency>,
    #[serde(default)]
    pub write_locks: Vec<TaskWriteLock>,
    #[serde(default)]
    pub thread_lineage: Vec<TaskThreadLineage>,
    #[serde(default)]
    pub task_run_thread_bindings: Vec<TaskRunThreadBinding>,
    #[serde(default)]
    pub task_run_turns: Vec<TaskRunTurn>,
    #[serde(default)]
    pub result_candidates: Vec<TaskResultCandidate>,
    #[serde(default)]
    pub result_review_events: Vec<TaskResultReviewEvent>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskListParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_kind: Option<TaskOwnerKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<TaskStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskListResponse {
    #[serde(default)]
    pub tasks: Vec<Task>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskTreeParams {
    pub task_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskTreeResponse {
    pub tree: TaskTree,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskEventsParams {
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_sequence: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskEventsResponse {
    pub task_id: String,
    #[serde(default)]
    pub events: Vec<TaskEvent>,
    pub last_sequence: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskCancelParams {
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default)]
    pub scope: TaskCancelScope,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskCancelResponse {
    pub task: Task,
    #[serde(default)]
    pub cancelled_tasks: Vec<Task>,
    #[serde(default)]
    pub detached_tasks: Vec<Task>,
    #[serde(default)]
    pub kept_tasks: Vec<Task>,
    #[serde(default)]
    pub cancelled_runs: Vec<TaskRun>,
    #[serde(default)]
    pub cancelled_deliveries: Vec<TaskDelivery>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRescheduleParams {
    pub task_id: String,
    pub trigger: TaskTriggerInput,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRescheduleResponse {
    pub task: Task,
    pub trigger: TaskTrigger,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskPauseParams {
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskPauseResponse {
    pub task: Task,
    #[serde(default)]
    pub triggers: Vec<TaskTrigger>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskResumeParams {
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskResumeResponse {
    pub task: Task,
    #[serde(default)]
    pub triggers: Vec<TaskTrigger>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskDetachParams {
    pub task_id: String,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDetachResponse {
    pub task: Task,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskAcceptParams {
    pub task_id: String,
    pub run_id: String,
    pub candidate_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAcceptResponse {
    pub task: Task,
    pub run: TaskRun,
    pub candidate: TaskResultCandidate,
    pub review_event: TaskResultReviewEvent,
    pub result: TaskResult,
    pub accepted: bool,
    pub already_accepted: bool,
    pub status: TaskStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_turn_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskReviseParams {
    pub task_id: String,
    pub run_id: String,
    pub candidate_id: String,
    pub feedback: String,
    #[serde(default)]
    pub additional_instructions: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskReviseResponse {
    pub task: Task,
    pub run: TaskRun,
    pub candidate: TaskResultCandidate,
    pub review_event: TaskResultReviewEvent,
    pub task_run_turn: TaskRunTurn,
    pub requested: bool,
    pub already_requested: bool,
    pub status: TaskStatus,
    pub child_thread_id: String,
    pub child_turn_id: String,
    pub round: u32,
    pub feedback: String,
    #[serde(default)]
    pub additional_instructions: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskWaitParams {
    #[serde(default)]
    pub task_ids: Vec<String>,
    #[serde(default)]
    pub run_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub mode: TaskWaitMode,
    #[serde(default)]
    pub return_completed: bool,
    #[serde(default)]
    pub return_pending: bool,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskWaitMode {
    AllTerminal,
    AnyTerminal,
    AllTerminalOrReviewRequired,
    AnyTerminalOrReviewRequired,
}

impl Default for TaskWaitMode {
    fn default() -> Self {
        Self::AllTerminal
    }
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskWaitItem {
    pub task: Task,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<TaskRun>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_turn_id: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskWaitReviewAction {
    TaskAccept,
    TaskRevise,
    TaskCancel,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskWaitRevisionBlockedReason {
    MaxRevisionRoundsReached,
    CandidateNotRevisable,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskWaitReviewItem {
    pub item: TaskWaitItem,
    pub candidate: TaskResultCandidate,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_policy: Option<TaskAgentReviewPolicy>,
    #[serde(default)]
    pub max_revision_rounds: u32,
    #[serde(default)]
    pub remaining_revision_rounds: u32,
    #[serde(default)]
    pub allowed_actions: Vec<TaskWaitReviewAction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_blocked_reason: Option<TaskWaitRevisionBlockedReason>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskWaitNonWaitableReason {
    FutureScheduledTaskWithoutActiveRun,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskWaitNonWaitableItem {
    pub item: TaskWaitItem,
    pub reason: TaskWaitNonWaitableReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_fire_at: Option<i64>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskWaitResponse {
    #[serde(default)]
    pub completed: Vec<TaskWaitItem>,
    #[serde(default)]
    pub failed: Vec<TaskWaitItem>,
    #[serde(default)]
    pub cancelled: Vec<TaskWaitItem>,
    #[serde(default)]
    pub review_required: Vec<TaskWaitReviewItem>,
    #[serde(default)]
    pub pending: Vec<TaskWaitItem>,
    #[serde(default)]
    pub non_waitable: Vec<TaskWaitNonWaitableItem>,
    pub timed_out: bool,
    pub total_count: u32,
    pub terminal_count: u32,
    pub pending_count: u32,
    #[serde(default)]
    pub review_required_count: u32,
    #[serde(default)]
    pub non_waitable_count: u32,
    pub mode: TaskWaitMode,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgendaParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_kind: Option<TaskOwnerKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<i64>,
    #[serde(default)]
    pub statuses: Vec<TaskStatus>,
    #[serde(default)]
    pub trigger_kinds: Vec<TaskTriggerKind>,
    #[serde(default)]
    pub include_paused: bool,
    #[serde(default)]
    pub include_completed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgendaItem {
    pub task: Task,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger: Option<TaskTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_run: Option<TaskRun>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_delivery: Option<TaskDelivery>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_kind: Option<TaskTriggerKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_status: Option<TaskTriggerStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_fire_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_fire_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    pub recurring: bool,
    pub delivery_mode: TaskDeliveryMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_preview: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskAgendaResponse {
    #[serde(default)]
    pub items: Vec<TaskAgendaItem>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct TaskDeliveriesParams {
    pub workspace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default)]
    pub statuses: Vec<TaskDeliveryStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDeliveriesResponse {
    #[serde(default)]
    pub deliveries: Vec<TaskDelivery>,
    #[serde(default)]
    pub attempts: Vec<TaskDeliveryAttempt>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskNotificationContext {
    pub workspace_id: String,
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub event_id: String,
    pub sequence: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskProgressDetails {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<TaskValue>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskCreatedNotification {
    pub context: TaskNotificationContext,
    pub task: Task,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskScheduledNotification {
    pub context: TaskNotificationContext,
    pub trigger: TaskTrigger,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskQueuedNotification {
    pub context: TaskNotificationContext,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<TaskRun>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRunCreatedNotification {
    pub context: TaskNotificationContext,
    pub run: TaskRun,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRunStartedNotification {
    pub context: TaskNotificationContext,
    pub run: TaskRun,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskProgressNotification {
    pub context: TaskNotificationContext,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<TaskProgressDetails>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRunCompletedNotification {
    pub context: TaskNotificationContext,
    pub run: TaskRun,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRunFailedNotification {
    pub context: TaskNotificationContext,
    pub run: TaskRun,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskCompletedNotification {
    pub context: TaskNotificationContext,
    pub task: Task,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskFailedNotification {
    pub context: TaskNotificationContext,
    pub task: Task,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskCancelledNotification {
    pub context: TaskNotificationContext,
    pub task: Task,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDetachedNotification {
    pub context: TaskNotificationContext,
    pub task: Task,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskUpdatedNotification {
    pub context: TaskNotificationContext,
    pub task: Task,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger: Option<TaskTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_spec: Option<TaskAgentSpec>,
    #[serde(default)]
    pub changed_fields: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRescheduledNotification {
    pub context: TaskNotificationContext,
    pub trigger: TaskTrigger,
    pub reason: TaskRescheduleReason,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskPausedNotification {
    pub context: TaskNotificationContext,
    pub task: Task,
    #[serde(default)]
    pub triggers: Vec<TaskTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskResumedNotification {
    pub context: TaskNotificationContext,
    pub task: Task,
    #[serde(default)]
    pub triggers: Vec<TaskTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDeliveryQueuedNotification {
    pub context: TaskNotificationContext,
    pub delivery: TaskDelivery,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_preview: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDeliveryStartedNotification {
    pub context: TaskNotificationContext,
    pub delivery: TaskDelivery,
    pub attempt: TaskDeliveryAttempt,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDeliveryDeliveredNotification {
    pub context: TaskNotificationContext,
    pub delivery: TaskDelivery,
    pub attempt: TaskDeliveryAttempt,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDeliveryFailedNotification {
    pub context: TaskNotificationContext,
    pub delivery: TaskDelivery,
    pub attempt: TaskDeliveryAttempt,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskDeliveryCancelledNotification {
    pub context: TaskNotificationContext,
    pub delivery: TaskDelivery,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskTreeChangedNotification {
    pub context: TaskNotificationContext,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TaskRecoveredNotification {
    pub context: TaskNotificationContext,
    pub recovered_at: i64,
}

#[derive(Serialize, Deserialize, JsonSchema, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TaskTurnItem {
    pub id: String,
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_task_id: Option<String>,
    pub title: String,
    pub status: TaskStatus,
    pub trigger_kind: TaskTriggerKind,
    pub executor_kind: TaskExecutorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_role: Option<String>,
    pub depth: i64,
    pub max_depth: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_fire_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_preview: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_preview: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[cfg(test)]
mod tests {
    use super::{
        TaskAgentSpec, TaskEventPayload, TaskExecutorKind, TaskOwnerKind, TaskRescheduleReason,
        TaskRunExecutionStatus, TaskRunStatus, TaskStatus, TaskTriggerKind, TaskTriggerSpec,
        TaskTriggerStatus, TaskTurnItem, TaskWaitMode, TaskWaitResponse, TaskWaitReviewAction,
        TaskWaitRevisionBlockedReason,
    };
    use serde_json::json;

    #[test]
    fn task_enums_serialize_as_snake_case() {
        assert_eq!(
            serde_json::to_value(TaskExecutorKind::Agent).expect("kind should encode"),
            json!("agent")
        );
        assert_eq!(
            serde_json::to_value(TaskTriggerKind::ScheduledAt).expect("kind should encode"),
            json!("scheduled_at")
        );
        assert_eq!(
            serde_json::to_value(TaskTriggerKind::Interval).expect("kind should encode"),
            json!("interval")
        );
    }

    #[test]
    fn task_trigger_spec_is_tagged_by_kind() {
        let spec = TaskTriggerSpec::ScheduledAt {
            scheduled_at: 1_700_000_000,
            timezone: Some("Europe/Moscow".to_owned()),
            catch_up_policy: None,
        };

        let encoded = serde_json::to_value(&spec).expect("trigger spec should encode");
        assert_eq!(encoded["kind"], json!("scheduled_at"));
        assert_eq!(encoded["scheduled_at"], json!(1_700_000_000));
        assert_eq!(spec.kind(), TaskTriggerKind::ScheduledAt);

        let decoded: TaskTriggerSpec =
            serde_json::from_value(encoded).expect("trigger spec should decode");
        assert_eq!(decoded, spec);
    }

    #[test]
    fn task_rescheduled_without_reason_decodes_as_unknown() {
        let decoded: TaskEventPayload = serde_json::from_value(json!({
            "kind": "task_rescheduled",
            "payload": {
                "task_id": "task_1",
                "trigger": {
                    "id": "trigger_1",
                    "taskId": "task_1",
                    "status": "active",
                    "spec": {
                        "kind": "cron",
                        "cron_expr": "0 7 * * *",
                        "timezone": "Europe/Moscow"
                    },
                    "nextFireAt": 1_700_000_000,
                    "lastFireAt": 1_699_913_600,
                    "createdAt": 1_699_900_000,
                    "updatedAt": 1_699_913_600
                },
                "rescheduled_at": 1_699_913_600
            }
        }))
        .expect("legacy task/rescheduled payload without reason should decode");

        let TaskEventPayload::TaskRescheduled { reason, .. } = decoded else {
            panic!("expected task rescheduled event");
        };
        assert_eq!(reason, TaskRescheduleReason::Unknown);
    }

    #[test]
    fn task_turn_item_round_trips_with_camel_case_fields() {
        let item = TaskTurnItem {
            id: "item_1".to_owned(),
            task_id: "task_1".to_owned(),
            run_id: Some("run_1".to_owned()),
            parent_task_id: None,
            root_task_id: Some("task_root".to_owned()),
            title: "Check weather".to_owned(),
            status: TaskStatus::Scheduled,
            trigger_kind: TaskTriggerKind::ScheduledAt,
            executor_kind: TaskExecutorKind::Agent,
            child_thread_id: None,
            child_turn_id: None,
            agent_role: Some("worker".to_owned()),
            depth: 0,
            max_depth: 3,
            next_fire_at: Some(1_700_000_000),
            result_preview: None,
            error_preview: None,
            created_at: 1_700_000_000,
            updated_at: 1_700_000_000,
        };

        let encoded = serde_json::to_value(&item).expect("item should encode");
        assert_eq!(encoded["taskId"], json!("task_1"));
        assert_eq!(encoded["triggerKind"], json!("scheduled_at"));

        let decoded: TaskTurnItem = serde_json::from_value(encoded).expect("item should decode");
        assert_eq!(decoded, item);
    }

    #[test]
    fn task_trigger_status_round_trips() {
        let encoded =
            serde_json::to_value(TaskTriggerStatus::Active).expect("status should encode");
        assert_eq!(encoded, json!("active"));

        let decoded: TaskOwnerKind =
            serde_json::from_value(json!("workspace")).expect("owner should decode");
        assert_eq!(decoded, TaskOwnerKind::Workspace);
    }

    #[test]
    fn waiting_review_statuses_are_nonterminal() {
        assert!(!TaskStatus::WaitingReview.is_terminal());
        assert!(!TaskRunStatus::WaitingReview.is_terminal());
        assert!(!TaskRunExecutionStatus::WaitingReview.is_terminal());
    }

    #[test]
    fn legacy_task_wait_response_without_review_fields_decodes() {
        let decoded: TaskWaitResponse = serde_json::from_value(json!({
            "completed": [],
            "failed": [],
            "cancelled": [],
            "pending": [],
            "nonWaitable": [],
            "timedOut": false,
            "totalCount": 0,
            "terminalCount": 0,
            "pendingCount": 0,
            "mode": "all_terminal"
        }))
        .expect("legacy task wait response should decode");

        assert!(decoded.review_required.is_empty());
        assert_eq!(decoded.review_required_count, 0);
        assert_eq!(decoded.mode, TaskWaitMode::AllTerminal);
    }

    #[test]
    fn phase_12_task_wait_review_response_round_trips() {
        let decoded: TaskWaitResponse = serde_json::from_value(json!({
            "completed": [],
            "failed": [],
            "cancelled": [],
            "reviewRequired": [{
                "item": {
                    "task": {
                        "id": "task_review00000001",
                        "workspaceId": "workspace_default",
                        "ownerKind": "thread",
                        "ownerId": "thread_parent000001",
                        "executorKind": "agent",
                        "status": "waiting_review",
                        "title": "Review child work",
                        "goal": "Produce a result",
                        "priority": 0,
                        "revision": 1,
                        "createdAt": 10,
                        "updatedAt": 20
                    },
                    "childThreadId": "thread_child0000001",
                    "childTurnId": "turn_child000000001"
                },
                "candidate": {
                    "id": "candidate_review_0001",
                    "taskId": "task_review00000001",
                    "runId": "run_review000000001",
                    "taskRunTurnId": "task_run_turn_000001",
                    "threadId": "thread_child0000001",
                    "turnId": "turn_child000000001",
                    "round": 0,
                    "status": "pending_review",
                    "result": {
                        "summary": "done",
                        "data": {
                            "kind": "string",
                            "value": "result"
                        }
                    },
                    "summary": "done",
                    "diagnostics": [],
                    "createdAt": 30,
                    "updatedAt": 30
                },
                "maxRevisionRounds": 2,
                "remainingRevisionRounds": 1,
                "allowedActions": ["task_accept", "task_revise", "task_cancel"]
            }],
            "pending": [],
            "nonWaitable": [],
            "timedOut": false,
            "totalCount": 1,
            "terminalCount": 0,
            "pendingCount": 0,
            "reviewRequiredCount": 1,
            "nonWaitableCount": 0,
            "mode": "all_terminal_or_review_required"
        }))
        .expect("review task wait response should decode");

        assert_eq!(decoded.review_required_count, 1);
        assert_eq!(
            decoded.review_required[0].candidate.summary.as_deref(),
            Some("done")
        );
        assert_eq!(
            decoded.review_required[0].allowed_actions,
            vec![
                TaskWaitReviewAction::TaskAccept,
                TaskWaitReviewAction::TaskRevise,
                TaskWaitReviewAction::TaskCancel,
            ]
        );
        assert_eq!(decoded.mode, TaskWaitMode::AllTerminalOrReviewRequired);

        let encoded = serde_json::to_value(&decoded).expect("response should encode");
        assert_eq!(encoded["reviewRequiredCount"], json!(1));
        assert_eq!(
            encoded["reviewRequired"][0]["allowedActions"],
            json!(["task_accept", "task_revise", "task_cancel"])
        );
    }

    #[test]
    fn task_wait_review_blocked_reason_serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_value(TaskWaitRevisionBlockedReason::MaxRevisionRoundsReached)
                .expect("blocked reason should encode"),
            json!("max_revision_rounds_reached")
        );
    }

    #[test]
    fn legacy_task_agent_spec_without_review_policy_decodes() {
        let decoded: TaskAgentSpec = serde_json::from_value(json!({
            "id": "agent_spec_legacy01",
            "taskId": "task_legacy0000001",
            "runId": null,
            "agentRole": "worker",
            "agentNickname": "Worker",
            "model": "test-model",
            "modelProvider": "openai",
            "prompt": {
                "goal": "Do work",
                "instructions": [],
                "input": null,
                "outputInstructions": null
            },
            "contextPolicy": null,
            "toolPolicy": null,
            "resultContract": null,
            "depth": 1,
            "maxDepth": 3,
            "createdAt": 1,
            "updatedAt": 1
        }))
        .expect("legacy task agent spec should decode");

        assert!(decoded.review_policy.is_none());
    }
}
