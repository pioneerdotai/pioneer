use anyhow::{Result, bail};
use pioneer_hooks::HookRunStatus;
use pioneer_protocol::{
    MemoryActorKind, MemoryCandidateStatus, MemoryCategory, MemoryScopeKind, MemorySensitivity,
    MemorySourceKind, MemoryStatus, PromptManifestProfile, ProviderFailureClass,
    ProviderFailureStage, RecoveryAction, RecoveryJobStatus, RecoveryTrigger, SandboxMode,
    TaskConcurrencyConflictPolicy, TaskDeliveryAttemptStatus, TaskDeliveryMode, TaskDeliveryStatus,
    TaskExecutorKind, TaskOwnerKind, TaskRunExecutionStatus, TaskRunStatus, TaskStatus,
    TaskTriggerKind, TaskTriggerStatus, TaskWriteLockScopeKind, TaskWriteLockStatus, ThreadMode,
    ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, TurnItem, TurnItemAttemptStatus,
    TurnItemTimeoutReason, TurnItemType, TurnStatus, UserInput,
};

pub const DB_ID_LEN: usize = 21;

pub const TURN_ITEM_STATUS_IN_PROGRESS: &str = "in_progress";
pub const TURN_ITEM_STATUS_COMPLETED: &str = "completed";
pub const TURN_ITEM_STATUS_FAILED: &str = "failed";
pub const TURN_ITEM_STATUS_TIMED_OUT: &str = "timed_out";
pub const TURN_ITEM_STATUS_CANCELLED: &str = "cancelled";

pub const ATTEMPT_STATUS_RUNNING: &str = "running";
pub const ATTEMPT_STATUS_COMPLETED: &str = "completed";
pub const ATTEMPT_STATUS_FAILED: &str = "failed";
pub const ATTEMPT_STATUS_TIMED_OUT: &str = "timed_out";
pub const ATTEMPT_STATUS_CANCELLED: &str = "cancelled";
pub const ATTEMPT_STATUS_INTERRUPTED: &str = "interrupted";
pub const ATTEMPT_STATUS_RETRYING: &str = "retrying";
pub const ATTEMPT_STATUS_EXHAUSTED: &str = "exhausted";

pub const RECOVERY_STATUS_PENDING: &str = "pending";
pub const RECOVERY_STATUS_ACTIVE: &str = "active";
pub const RECOVERY_STATUS_SUCCEEDED: &str = "succeeded";
pub const RECOVERY_STATUS_FAILED: &str = "failed";
pub const RECOVERY_STATUS_EXHAUSTED: &str = "exhausted";
pub const RECOVERY_STATUS_CANCELLED: &str = "cancelled";

pub const MEMORY_NAMESPACE_DEFAULT: &str = "default";
pub const MEMORY_SCOPE_SLOT_PRIMARY: &str = "primary";
pub const MEMORY_REPAIR_STATUS_OK: &str = "ok";
pub const MEMORY_REPAIR_STATUS_REPAIR_NEEDED: &str = "repair_needed";
pub const MEMORY_REPAIR_STATUS_FAILED: &str = "failed";

pub const MEMORY_EVENT_CREATED: &str = "created";
pub const MEMORY_EVENT_UPDATED: &str = "updated";
pub const MEMORY_EVENT_FORGOTTEN: &str = "forgotten";
pub const MEMORY_EVENT_SUPERSEDED: &str = "superseded";
pub const MEMORY_EVENT_EXPIRED: &str = "expired";
pub const MEMORY_EVENT_ACCESSED: &str = "accessed";
pub const MEMORY_EVENT_REPAIR_STATUS_CHANGED: &str = "repair_status_changed";
pub const MEMORY_EVENT_CAPSULE_REPAIR_STATUS_CHANGED: &str = "capsule_repair_status_changed";
pub const MEMORY_EVENT_CANDIDATE_CREATED: &str = "candidate_created";
pub const MEMORY_EVENT_CANDIDATE_APPROVED: &str = "candidate_approved";
pub const MEMORY_EVENT_CANDIDATE_REJECTED: &str = "candidate_rejected";
pub const MEMORY_EVENT_CANDIDATE_EXPIRED: &str = "candidate_expired";

pub const MEMORY_CAPSULE_STATUS_MISSING: &str = "missing";
pub const MEMORY_CAPSULE_STATUS_REPAIR_NEEDED: &str = "repair_needed";

pub const MEMORY_REPAIR_JOB_STATUS_PENDING: &str = "pending";
pub const MEMORY_REPAIR_JOB_STATUS_RUNNING: &str = "running";
pub const MEMORY_REPAIR_JOB_STATUS_COMPLETED: &str = "completed";
pub const MEMORY_REPAIR_JOB_STATUS_FAILED: &str = "failed";

pub const HOOK_RUN_STATUS_QUEUED: &str = "queued";
pub const HOOK_RUN_STATUS_RUNNING: &str = "running";
pub const HOOK_RUN_STATUS_SUCCEEDED: &str = "succeeded";
pub const HOOK_RUN_STATUS_FAILED: &str = "failed";
pub const HOOK_RUN_STATUS_TIMED_OUT: &str = "timed_out";
pub const HOOK_RUN_STATUS_SKIPPED: &str = "skipped";

pub fn hook_run_status_to_db(status: HookRunStatus) -> &'static str {
    match status {
        HookRunStatus::Queued => HOOK_RUN_STATUS_QUEUED,
        HookRunStatus::Running => HOOK_RUN_STATUS_RUNNING,
        HookRunStatus::Succeeded => HOOK_RUN_STATUS_SUCCEEDED,
        HookRunStatus::Failed => HOOK_RUN_STATUS_FAILED,
        HookRunStatus::TimedOut => HOOK_RUN_STATUS_TIMED_OUT,
        HookRunStatus::Skipped => HOOK_RUN_STATUS_SKIPPED,
    }
}

pub fn hook_run_status_from_db(value: &str) -> Result<HookRunStatus> {
    match value {
        HOOK_RUN_STATUS_QUEUED => Ok(HookRunStatus::Queued),
        HOOK_RUN_STATUS_RUNNING => Ok(HookRunStatus::Running),
        HOOK_RUN_STATUS_SUCCEEDED => Ok(HookRunStatus::Succeeded),
        HOOK_RUN_STATUS_FAILED => Ok(HookRunStatus::Failed),
        HOOK_RUN_STATUS_TIMED_OUT => Ok(HookRunStatus::TimedOut),
        HOOK_RUN_STATUS_SKIPPED => Ok(HookRunStatus::Skipped),
        _ => bail!("unknown hook run status `{value}`"),
    }
}

pub fn memory_scope_kind_to_db(kind: MemoryScopeKind) -> &'static str {
    match kind {
        MemoryScopeKind::User => "user",
        MemoryScopeKind::Workspace => "workspace",
        MemoryScopeKind::Thread => "thread",
        MemoryScopeKind::Agent => "agent",
        MemoryScopeKind::Task => "task",
    }
}

pub fn memory_scope_kind_from_db(value: &str) -> Result<MemoryScopeKind> {
    match value {
        "user" => Ok(MemoryScopeKind::User),
        "workspace" => Ok(MemoryScopeKind::Workspace),
        "thread" => Ok(MemoryScopeKind::Thread),
        "agent" => Ok(MemoryScopeKind::Agent),
        "task" => Ok(MemoryScopeKind::Task),
        _ => bail!("unknown memory scope kind `{value}`"),
    }
}

pub fn memory_category_to_db(category: MemoryCategory) -> &'static str {
    match category {
        MemoryCategory::Identity => "identity",
        MemoryCategory::Preference => "preference",
        MemoryCategory::Biography => "biography",
        MemoryCategory::Relationship => "relationship",
        MemoryCategory::RecurringInstruction => "recurring_instruction",
        MemoryCategory::ProjectPolicy => "project_policy",
        MemoryCategory::ProjectFact => "project_fact",
        MemoryCategory::ProjectDecision => "project_decision",
        MemoryCategory::Procedure => "procedure",
        MemoryCategory::Todo => "todo",
        MemoryCategory::Constraint => "constraint",
        MemoryCategory::CommunicationStyle => "communication_style",
        MemoryCategory::Custom => "custom",
    }
}

pub fn memory_category_from_db(value: &str) -> Result<MemoryCategory> {
    match value {
        "identity" => Ok(MemoryCategory::Identity),
        "preference" => Ok(MemoryCategory::Preference),
        "biography" => Ok(MemoryCategory::Biography),
        "relationship" => Ok(MemoryCategory::Relationship),
        "recurring_instruction" => Ok(MemoryCategory::RecurringInstruction),
        "project_policy" => Ok(MemoryCategory::ProjectPolicy),
        "project_fact" => Ok(MemoryCategory::ProjectFact),
        "project_decision" => Ok(MemoryCategory::ProjectDecision),
        "procedure" => Ok(MemoryCategory::Procedure),
        "todo" => Ok(MemoryCategory::Todo),
        "constraint" => Ok(MemoryCategory::Constraint),
        "communication_style" => Ok(MemoryCategory::CommunicationStyle),
        "custom" => Ok(MemoryCategory::Custom),
        _ => bail!("unknown memory category `{value}`"),
    }
}

pub fn memory_status_to_db(status: MemoryStatus) -> &'static str {
    match status {
        MemoryStatus::Active => "active",
        MemoryStatus::Superseded => "superseded",
        MemoryStatus::Deleted => "deleted",
        MemoryStatus::Expired => "expired",
    }
}

pub fn memory_status_from_db(value: &str) -> Result<MemoryStatus> {
    match value {
        "active" => Ok(MemoryStatus::Active),
        "superseded" => Ok(MemoryStatus::Superseded),
        "deleted" => Ok(MemoryStatus::Deleted),
        "expired" => Ok(MemoryStatus::Expired),
        _ => bail!("unknown memory status `{value}`"),
    }
}

pub fn memory_sensitivity_to_db(sensitivity: MemorySensitivity) -> &'static str {
    match sensitivity {
        MemorySensitivity::Normal => "normal",
        MemorySensitivity::Personal => "personal",
        MemorySensitivity::SecretLike => "secret_like",
        MemorySensitivity::Regulated => "regulated",
    }
}

pub fn memory_sensitivity_from_db(value: &str) -> Result<MemorySensitivity> {
    match value {
        "normal" => Ok(MemorySensitivity::Normal),
        "personal" => Ok(MemorySensitivity::Personal),
        "secret_like" => Ok(MemorySensitivity::SecretLike),
        "regulated" => Ok(MemorySensitivity::Regulated),
        _ => bail!("unknown memory sensitivity `{value}`"),
    }
}

pub fn memory_source_kind_to_db(kind: MemorySourceKind) -> &'static str {
    match kind {
        MemorySourceKind::ExplicitUserRequest => "explicit_user_request",
        MemorySourceKind::UserCorrection => "user_correction",
        MemorySourceKind::AssistantInference => "assistant_inference",
        MemorySourceKind::BackgroundExtractor => "background_extractor",
        MemorySourceKind::ToolObservation => "tool_observation",
        MemorySourceKind::Import => "import",
        MemorySourceKind::System => "system",
    }
}

pub fn memory_source_kind_from_db(value: &str) -> Result<MemorySourceKind> {
    match value {
        "explicit_user_request" => Ok(MemorySourceKind::ExplicitUserRequest),
        "user_correction" => Ok(MemorySourceKind::UserCorrection),
        "assistant_inference" => Ok(MemorySourceKind::AssistantInference),
        "background_extractor" => Ok(MemorySourceKind::BackgroundExtractor),
        "tool_observation" => Ok(MemorySourceKind::ToolObservation),
        "import" => Ok(MemorySourceKind::Import),
        "system" => Ok(MemorySourceKind::System),
        _ => bail!("unknown memory source kind `{value}`"),
    }
}

pub fn memory_actor_kind_to_db(kind: MemoryActorKind) -> &'static str {
    match kind {
        MemoryActorKind::User => "user",
        MemoryActorKind::Assistant => "assistant",
        MemoryActorKind::Extractor => "extractor",
        MemoryActorKind::System => "system",
        MemoryActorKind::Tool => "tool",
    }
}

pub fn memory_actor_kind_from_db(value: &str) -> Result<MemoryActorKind> {
    match value {
        "user" => Ok(MemoryActorKind::User),
        "assistant" => Ok(MemoryActorKind::Assistant),
        "extractor" => Ok(MemoryActorKind::Extractor),
        "system" => Ok(MemoryActorKind::System),
        "tool" => Ok(MemoryActorKind::Tool),
        _ => bail!("unknown memory actor kind `{value}`"),
    }
}

pub fn memory_candidate_status_to_db(status: MemoryCandidateStatus) -> &'static str {
    match status {
        MemoryCandidateStatus::Pending => "pending",
        MemoryCandidateStatus::PendingSilent => "pending_silent",
        MemoryCandidateStatus::AskOnUse => "ask_on_use",
        MemoryCandidateStatus::NeedsReview => "needs_review",
        MemoryCandidateStatus::Approved => "approved",
        MemoryCandidateStatus::Rejected => "rejected",
        MemoryCandidateStatus::AutoRejected => "auto_rejected",
        MemoryCandidateStatus::ReviewDisabledRejected => "review_disabled_rejected",
        MemoryCandidateStatus::Superseded => "superseded",
        MemoryCandidateStatus::MergedDuplicate => "merged_duplicate",
        MemoryCandidateStatus::Expired => "expired",
    }
}

pub fn memory_candidate_status_from_db(value: &str) -> Result<MemoryCandidateStatus> {
    match value {
        "pending" => Ok(MemoryCandidateStatus::Pending),
        "pending_silent" => Ok(MemoryCandidateStatus::PendingSilent),
        "ask_on_use" => Ok(MemoryCandidateStatus::AskOnUse),
        "needs_review" => Ok(MemoryCandidateStatus::NeedsReview),
        "approved" => Ok(MemoryCandidateStatus::Approved),
        "rejected" => Ok(MemoryCandidateStatus::Rejected),
        "auto_rejected" => Ok(MemoryCandidateStatus::AutoRejected),
        "review_disabled_rejected" => Ok(MemoryCandidateStatus::ReviewDisabledRejected),
        "superseded" => Ok(MemoryCandidateStatus::Superseded),
        "merged_duplicate" => Ok(MemoryCandidateStatus::MergedDuplicate),
        "expired" => Ok(MemoryCandidateStatus::Expired),
        _ => bail!("unknown memory candidate status `{value}`"),
    }
}

pub fn sandbox_mode_to_db(mode: SandboxMode) -> &'static str {
    match mode {
        SandboxMode::FullAccess => "full_access",
    }
}

pub fn thread_mode_to_db(mode: ThreadMode) -> &'static str {
    match mode {
        ThreadMode::Chat => "chat",
        ThreadMode::Agent => "agent",
    }
}

pub fn thread_status_to_db(status: ThreadStatus) -> &'static str {
    match status {
        ThreadStatus::Active => "active",
        ThreadStatus::Idle => "idle",
        ThreadStatus::Closed => "closed",
    }
}

pub fn thread_origin_kind_to_db(kind: ThreadOriginKind) -> &'static str {
    match kind {
        ThreadOriginKind::User => "user",
        ThreadOriginKind::TaskRun => "task_run",
        ThreadOriginKind::System => "system",
    }
}

pub fn thread_origin_kind_from_db(value: &str) -> Option<ThreadOriginKind> {
    match value {
        "user" => Some(ThreadOriginKind::User),
        "task_run" => Some(ThreadOriginKind::TaskRun),
        "system" => Some(ThreadOriginKind::System),
        _ => None,
    }
}

pub fn thread_sidebar_visibility_to_db(visibility: ThreadSidebarVisibility) -> &'static str {
    match visibility {
        ThreadSidebarVisibility::Visible => "visible",
        ThreadSidebarVisibility::Hidden => "hidden",
    }
}

pub fn thread_sidebar_visibility_from_db(value: &str) -> Option<ThreadSidebarVisibility> {
    match value {
        "visible" => Some(ThreadSidebarVisibility::Visible),
        "hidden" => Some(ThreadSidebarVisibility::Hidden),
        _ => None,
    }
}

pub fn thread_mode_from_db(value: &str) -> Option<ThreadMode> {
    match value {
        "chat" => Some(ThreadMode::Chat),
        "agent" => Some(ThreadMode::Agent),
        _ => None,
    }
}

pub fn thread_status_from_db(value: &str) -> Option<ThreadStatus> {
    match value {
        "active" => Some(ThreadStatus::Active),
        "idle" => Some(ThreadStatus::Idle),
        "closed" => Some(ThreadStatus::Closed),
        _ => None,
    }
}

pub fn task_status_to_db(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Draft => "draft",
        TaskStatus::Scheduled => "scheduled",
        TaskStatus::Queued => "queued",
        TaskStatus::Running => "running",
        TaskStatus::Waiting => "waiting",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
    }
}

pub fn task_status_from_db(value: &str) -> Option<TaskStatus> {
    match value {
        "draft" => Some(TaskStatus::Draft),
        "scheduled" => Some(TaskStatus::Scheduled),
        "queued" => Some(TaskStatus::Queued),
        "running" => Some(TaskStatus::Running),
        "waiting" => Some(TaskStatus::Waiting),
        "completed" => Some(TaskStatus::Completed),
        "failed" => Some(TaskStatus::Failed),
        "cancelled" => Some(TaskStatus::Cancelled),
        _ => None,
    }
}

pub fn is_terminal_task_status(status: TaskStatus) -> bool {
    status.is_terminal()
}

pub fn is_terminal_task_status_db(status: &str) -> bool {
    task_status_from_db(status).is_some_and(TaskStatus::is_terminal)
}

pub fn task_trigger_kind_to_db(kind: TaskTriggerKind) -> &'static str {
    match kind {
        TaskTriggerKind::Immediate => "immediate",
        TaskTriggerKind::ScheduledAt => "scheduled_at",
        TaskTriggerKind::Interval => "interval",
        TaskTriggerKind::Cron => "cron",
        TaskTriggerKind::Manual => "manual",
        TaskTriggerKind::External => "external",
        TaskTriggerKind::Dependency => "dependency",
    }
}

pub fn task_trigger_kind_from_db(value: &str) -> Option<TaskTriggerKind> {
    match value {
        "immediate" => Some(TaskTriggerKind::Immediate),
        "scheduled_at" => Some(TaskTriggerKind::ScheduledAt),
        "interval" => Some(TaskTriggerKind::Interval),
        "cron" => Some(TaskTriggerKind::Cron),
        "manual" => Some(TaskTriggerKind::Manual),
        "external" => Some(TaskTriggerKind::External),
        "dependency" => Some(TaskTriggerKind::Dependency),
        _ => None,
    }
}

pub fn task_trigger_status_to_db(status: TaskTriggerStatus) -> &'static str {
    match status {
        TaskTriggerStatus::Active => "active",
        TaskTriggerStatus::Paused => "paused",
        TaskTriggerStatus::Exhausted => "exhausted",
        TaskTriggerStatus::Cancelled => "cancelled",
    }
}

pub fn task_trigger_status_from_db(value: &str) -> Option<TaskTriggerStatus> {
    match value {
        "active" => Some(TaskTriggerStatus::Active),
        "paused" => Some(TaskTriggerStatus::Paused),
        "exhausted" => Some(TaskTriggerStatus::Exhausted),
        "cancelled" => Some(TaskTriggerStatus::Cancelled),
        _ => None,
    }
}

pub fn task_executor_kind_to_db(kind: TaskExecutorKind) -> &'static str {
    match kind {
        TaskExecutorKind::Agent => "agent",
        TaskExecutorKind::Tool => "tool",
        TaskExecutorKind::Workflow => "workflow",
        TaskExecutorKind::Webhook => "webhook",
        TaskExecutorKind::System => "system",
    }
}

pub fn task_executor_kind_from_db(value: &str) -> Option<TaskExecutorKind> {
    match value {
        "agent" => Some(TaskExecutorKind::Agent),
        "tool" => Some(TaskExecutorKind::Tool),
        "workflow" => Some(TaskExecutorKind::Workflow),
        "webhook" => Some(TaskExecutorKind::Webhook),
        "system" => Some(TaskExecutorKind::System),
        _ => None,
    }
}

pub fn task_run_status_to_db(status: TaskRunStatus) -> &'static str {
    match status {
        TaskRunStatus::Queued => "queued",
        TaskRunStatus::Starting => "starting",
        TaskRunStatus::Running => "running",
        TaskRunStatus::Waiting => "waiting",
        TaskRunStatus::Succeeded => "succeeded",
        TaskRunStatus::Failed => "failed",
        TaskRunStatus::Cancelled => "cancelled",
        TaskRunStatus::TimedOut => "timed_out",
    }
}

pub fn task_run_status_from_db(value: &str) -> Option<TaskRunStatus> {
    match value {
        "queued" => Some(TaskRunStatus::Queued),
        "starting" => Some(TaskRunStatus::Starting),
        "running" => Some(TaskRunStatus::Running),
        "waiting" => Some(TaskRunStatus::Waiting),
        "succeeded" => Some(TaskRunStatus::Succeeded),
        "failed" => Some(TaskRunStatus::Failed),
        "cancelled" => Some(TaskRunStatus::Cancelled),
        "timed_out" => Some(TaskRunStatus::TimedOut),
        _ => None,
    }
}

pub fn is_terminal_task_run_status(status: TaskRunStatus) -> bool {
    status.is_terminal()
}

pub fn is_terminal_task_run_status_db(status: &str) -> bool {
    task_run_status_from_db(status).is_some_and(TaskRunStatus::is_terminal)
}

pub fn task_run_execution_status_to_db(status: TaskRunExecutionStatus) -> &'static str {
    match status {
        TaskRunExecutionStatus::Reserved => "reserved",
        TaskRunExecutionStatus::Starting => "starting",
        TaskRunExecutionStatus::Running => "running",
        TaskRunExecutionStatus::Succeeded => "succeeded",
        TaskRunExecutionStatus::Failed => "failed",
        TaskRunExecutionStatus::Cancelled => "cancelled",
        TaskRunExecutionStatus::TimedOut => "timed_out",
    }
}

pub fn task_run_execution_status_from_db(value: &str) -> Option<TaskRunExecutionStatus> {
    match value {
        "reserved" => Some(TaskRunExecutionStatus::Reserved),
        "starting" => Some(TaskRunExecutionStatus::Starting),
        "running" => Some(TaskRunExecutionStatus::Running),
        "succeeded" => Some(TaskRunExecutionStatus::Succeeded),
        "failed" => Some(TaskRunExecutionStatus::Failed),
        "cancelled" => Some(TaskRunExecutionStatus::Cancelled),
        "timed_out" => Some(TaskRunExecutionStatus::TimedOut),
        _ => None,
    }
}

pub fn is_terminal_task_run_execution_status(status: TaskRunExecutionStatus) -> bool {
    status.is_terminal()
}

pub fn task_delivery_mode_to_db(mode: TaskDeliveryMode) -> &'static str {
    match mode {
        TaskDeliveryMode::None => "none",
        TaskDeliveryMode::OwnerThread => "owner_thread",
        TaskDeliveryMode::Thread => "thread",
        TaskDeliveryMode::UserNotification => "user_notification",
        TaskDeliveryMode::Webhook => "webhook",
    }
}

pub fn task_delivery_mode_from_db(value: &str) -> Option<TaskDeliveryMode> {
    match value {
        "none" => Some(TaskDeliveryMode::None),
        "owner_thread" => Some(TaskDeliveryMode::OwnerThread),
        "thread" => Some(TaskDeliveryMode::Thread),
        "user_notification" => Some(TaskDeliveryMode::UserNotification),
        "webhook" => Some(TaskDeliveryMode::Webhook),
        _ => None,
    }
}

pub fn task_delivery_status_to_db(status: TaskDeliveryStatus) -> &'static str {
    match status {
        TaskDeliveryStatus::Pending => "pending",
        TaskDeliveryStatus::Delivering => "delivering",
        TaskDeliveryStatus::Delivered => "delivered",
        TaskDeliveryStatus::Failed => "failed",
        TaskDeliveryStatus::Cancelled => "cancelled",
    }
}

pub fn task_delivery_status_from_db(value: &str) -> Option<TaskDeliveryStatus> {
    match value {
        "pending" => Some(TaskDeliveryStatus::Pending),
        "delivering" => Some(TaskDeliveryStatus::Delivering),
        "delivered" => Some(TaskDeliveryStatus::Delivered),
        "failed" => Some(TaskDeliveryStatus::Failed),
        "cancelled" => Some(TaskDeliveryStatus::Cancelled),
        _ => None,
    }
}

pub fn task_concurrency_conflict_policy_to_db(
    policy: TaskConcurrencyConflictPolicy,
) -> &'static str {
    match policy {
        TaskConcurrencyConflictPolicy::Queue => "queue",
        TaskConcurrencyConflictPolicy::Reject => "reject",
        TaskConcurrencyConflictPolicy::CancelExisting => "cancel_existing",
        TaskConcurrencyConflictPolicy::Allow => "allow",
    }
}

pub fn task_concurrency_conflict_policy_from_db(
    value: &str,
) -> Option<TaskConcurrencyConflictPolicy> {
    match value {
        "queue" => Some(TaskConcurrencyConflictPolicy::Queue),
        "reject" => Some(TaskConcurrencyConflictPolicy::Reject),
        "cancel_existing" => Some(TaskConcurrencyConflictPolicy::CancelExisting),
        "allow" => Some(TaskConcurrencyConflictPolicy::Allow),
        _ => None,
    }
}

pub fn task_write_lock_scope_kind_to_db(kind: TaskWriteLockScopeKind) -> &'static str {
    match kind {
        TaskWriteLockScopeKind::Workspace => "workspace",
        TaskWriteLockScopeKind::Path => "path",
    }
}

pub fn task_write_lock_scope_kind_from_db(value: &str) -> Option<TaskWriteLockScopeKind> {
    match value {
        "workspace" => Some(TaskWriteLockScopeKind::Workspace),
        "path" => Some(TaskWriteLockScopeKind::Path),
        _ => None,
    }
}

pub fn task_write_lock_status_to_db(status: TaskWriteLockStatus) -> &'static str {
    match status {
        TaskWriteLockStatus::Pending => "pending",
        TaskWriteLockStatus::Acquired => "acquired",
        TaskWriteLockStatus::Released => "released",
        TaskWriteLockStatus::Expired => "expired",
        TaskWriteLockStatus::Cancelled => "cancelled",
        TaskWriteLockStatus::Blocked => "blocked",
    }
}

pub fn task_write_lock_status_from_db(value: &str) -> Option<TaskWriteLockStatus> {
    match value {
        "pending" => Some(TaskWriteLockStatus::Pending),
        "acquired" => Some(TaskWriteLockStatus::Acquired),
        "released" => Some(TaskWriteLockStatus::Released),
        "expired" => Some(TaskWriteLockStatus::Expired),
        "cancelled" => Some(TaskWriteLockStatus::Cancelled),
        "blocked" => Some(TaskWriteLockStatus::Blocked),
        _ => None,
    }
}

pub fn task_delivery_attempt_status_to_db(status: TaskDeliveryAttemptStatus) -> &'static str {
    match status {
        TaskDeliveryAttemptStatus::Started => "started",
        TaskDeliveryAttemptStatus::Delivered => "delivered",
        TaskDeliveryAttemptStatus::Failed => "failed",
    }
}

pub fn task_delivery_attempt_status_from_db(value: &str) -> Option<TaskDeliveryAttemptStatus> {
    match value {
        "started" => Some(TaskDeliveryAttemptStatus::Started),
        "delivered" => Some(TaskDeliveryAttemptStatus::Delivered),
        "failed" => Some(TaskDeliveryAttemptStatus::Failed),
        _ => None,
    }
}

pub fn task_owner_kind_to_db(kind: TaskOwnerKind) -> &'static str {
    match kind {
        TaskOwnerKind::User => "user",
        TaskOwnerKind::Thread => "thread",
        TaskOwnerKind::Workspace => "workspace",
        TaskOwnerKind::System => "system",
    }
}

pub fn task_owner_kind_from_db(value: &str) -> Option<TaskOwnerKind> {
    match value {
        "user" => Some(TaskOwnerKind::User),
        "thread" => Some(TaskOwnerKind::Thread),
        "workspace" => Some(TaskOwnerKind::Workspace),
        "system" => Some(TaskOwnerKind::System),
        _ => None,
    }
}

pub fn turn_status_to_db(status: TurnStatus) -> &'static str {
    match status {
        TurnStatus::InProgress => "in_progress",
        TurnStatus::Completed => "completed",
        TurnStatus::Failed => "failed",
        TurnStatus::Interrupted => "interrupted",
    }
}

pub fn turn_status_from_db(value: &str) -> Option<TurnStatus> {
    match value {
        "in_progress" => Some(TurnStatus::InProgress),
        "completed" => Some(TurnStatus::Completed),
        "failed" => Some(TurnStatus::Failed),
        "interrupted" => Some(TurnStatus::Interrupted),
        _ => None,
    }
}

pub fn turn_kind_to_db(kind: pioneer_protocol::TurnKind) -> &'static str {
    match kind {
        pioneer_protocol::TurnKind::Conversation => "conversation",
        pioneer_protocol::TurnKind::TaskRun => "task_run",
    }
}

pub fn turn_kind_from_db(value: &str) -> Option<pioneer_protocol::TurnKind> {
    match value {
        "conversation" => Some(pioneer_protocol::TurnKind::Conversation),
        "task_run" => Some(pioneer_protocol::TurnKind::TaskRun),
        _ => None,
    }
}

pub fn turn_origin_to_db(origin: pioneer_protocol::TurnOrigin) -> &'static str {
    match origin {
        pioneer_protocol::TurnOrigin::User => "user",
        pioneer_protocol::TurnOrigin::ScheduledTask => "scheduled_task",
        pioneer_protocol::TurnOrigin::DetachedTask => "detached_task",
        pioneer_protocol::TurnOrigin::AttachedTask => "attached_task",
    }
}

pub fn turn_origin_from_db(value: &str) -> Option<pioneer_protocol::TurnOrigin> {
    match value {
        "user" => Some(pioneer_protocol::TurnOrigin::User),
        "scheduled_task" => Some(pioneer_protocol::TurnOrigin::ScheduledTask),
        "detached_task" => Some(pioneer_protocol::TurnOrigin::DetachedTask),
        "attached_task" => Some(pioneer_protocol::TurnOrigin::AttachedTask),
        _ => None,
    }
}

pub fn turn_item_id_and_type_to_db(item: &TurnItem) -> (&str, &'static str) {
    (item.item_id(), turn_item_type_to_db(item.item_type()))
}

pub fn turn_item_type_to_db(item_type: TurnItemType) -> &'static str {
    match item_type {
        TurnItemType::UserMessage => "user_message",
        TurnItemType::AgentMessage => "agent_message",
        TurnItemType::Reasoning => "reasoning",
        TurnItemType::SystemEvent => "system_event",
        TurnItemType::Task => "task",
        TurnItemType::CommandExecution => "command_execution",
        TurnItemType::FileChange => "file_change",
        TurnItemType::WebSearch => "web_search",
        TurnItemType::WebFetch => "web_fetch",
        TurnItemType::Download => "download",
        TurnItemType::DynamicToolCall => "dynamic_tool_call",
    }
}

pub fn turn_item_type_from_db(value: &str) -> Option<TurnItemType> {
    match value {
        "user_message" => Some(TurnItemType::UserMessage),
        "agent_message" => Some(TurnItemType::AgentMessage),
        "reasoning" => Some(TurnItemType::Reasoning),
        "system_event" => Some(TurnItemType::SystemEvent),
        "task" => Some(TurnItemType::Task),
        "command_execution" => Some(TurnItemType::CommandExecution),
        "file_change" => Some(TurnItemType::FileChange),
        "web_search" => Some(TurnItemType::WebSearch),
        "web_fetch" => Some(TurnItemType::WebFetch),
        "download" => Some(TurnItemType::Download),
        "dynamic_tool_call" => Some(TurnItemType::DynamicToolCall),
        _ => None,
    }
}

pub fn turn_item_attempt_status_to_db(status: TurnItemAttemptStatus) -> &'static str {
    match status {
        TurnItemAttemptStatus::Running => ATTEMPT_STATUS_RUNNING,
        TurnItemAttemptStatus::Completed => ATTEMPT_STATUS_COMPLETED,
        TurnItemAttemptStatus::Failed => ATTEMPT_STATUS_FAILED,
        TurnItemAttemptStatus::TimedOut => ATTEMPT_STATUS_TIMED_OUT,
        TurnItemAttemptStatus::Cancelled => ATTEMPT_STATUS_CANCELLED,
        TurnItemAttemptStatus::Interrupted => ATTEMPT_STATUS_INTERRUPTED,
        TurnItemAttemptStatus::Retrying => ATTEMPT_STATUS_RETRYING,
        TurnItemAttemptStatus::Exhausted => ATTEMPT_STATUS_EXHAUSTED,
    }
}

#[allow(dead_code)]
pub fn turn_item_attempt_status_from_db(value: &str) -> Option<TurnItemAttemptStatus> {
    match value {
        ATTEMPT_STATUS_RUNNING => Some(TurnItemAttemptStatus::Running),
        ATTEMPT_STATUS_COMPLETED => Some(TurnItemAttemptStatus::Completed),
        ATTEMPT_STATUS_FAILED => Some(TurnItemAttemptStatus::Failed),
        ATTEMPT_STATUS_TIMED_OUT => Some(TurnItemAttemptStatus::TimedOut),
        ATTEMPT_STATUS_CANCELLED => Some(TurnItemAttemptStatus::Cancelled),
        ATTEMPT_STATUS_INTERRUPTED => Some(TurnItemAttemptStatus::Interrupted),
        ATTEMPT_STATUS_RETRYING => Some(TurnItemAttemptStatus::Retrying),
        ATTEMPT_STATUS_EXHAUSTED => Some(TurnItemAttemptStatus::Exhausted),
        _ => None,
    }
}

pub fn turn_item_timeout_reason_to_db(reason: TurnItemTimeoutReason) -> &'static str {
    match reason {
        TurnItemTimeoutReason::StartDeadlineExceeded => "start_deadline_exceeded",
        TurnItemTimeoutReason::IdleDeadlineExceeded => "idle_deadline_exceeded",
        TurnItemTimeoutReason::HardDeadlineExceeded => "hard_deadline_exceeded",
        TurnItemTimeoutReason::LeaseExpired => "lease_expired",
    }
}

#[allow(dead_code)]
pub fn turn_item_timeout_reason_from_db(value: &str) -> Option<TurnItemTimeoutReason> {
    match value {
        "start_deadline_exceeded" => Some(TurnItemTimeoutReason::StartDeadlineExceeded),
        "idle_deadline_exceeded" => Some(TurnItemTimeoutReason::IdleDeadlineExceeded),
        "hard_deadline_exceeded" => Some(TurnItemTimeoutReason::HardDeadlineExceeded),
        "lease_expired" => Some(TurnItemTimeoutReason::LeaseExpired),
        _ => None,
    }
}

pub fn recovery_job_status_to_db(status: RecoveryJobStatus) -> &'static str {
    match status {
        RecoveryJobStatus::Pending => RECOVERY_STATUS_PENDING,
        RecoveryJobStatus::Active => RECOVERY_STATUS_ACTIVE,
        RecoveryJobStatus::Succeeded => RECOVERY_STATUS_SUCCEEDED,
        RecoveryJobStatus::Failed => RECOVERY_STATUS_FAILED,
        RecoveryJobStatus::Exhausted => RECOVERY_STATUS_EXHAUSTED,
        RecoveryJobStatus::Cancelled => RECOVERY_STATUS_CANCELLED,
    }
}

pub fn recovery_job_status_from_db(value: &str) -> Option<RecoveryJobStatus> {
    match value {
        RECOVERY_STATUS_PENDING => Some(RecoveryJobStatus::Pending),
        RECOVERY_STATUS_ACTIVE => Some(RecoveryJobStatus::Active),
        RECOVERY_STATUS_SUCCEEDED => Some(RecoveryJobStatus::Succeeded),
        RECOVERY_STATUS_FAILED => Some(RecoveryJobStatus::Failed),
        RECOVERY_STATUS_EXHAUSTED => Some(RecoveryJobStatus::Exhausted),
        RECOVERY_STATUS_CANCELLED => Some(RecoveryJobStatus::Cancelled),
        _ => None,
    }
}

pub fn recovery_trigger_to_db(trigger: RecoveryTrigger) -> &'static str {
    match trigger {
        RecoveryTrigger::Timeout => "timeout",
        RecoveryTrigger::ProviderError => "provider_error",
        RecoveryTrigger::Unknown => "unknown",
    }
}

pub fn recovery_trigger_from_db(value: &str) -> Option<RecoveryTrigger> {
    match value {
        "timeout" => Some(RecoveryTrigger::Timeout),
        "provider_error" => Some(RecoveryTrigger::ProviderError),
        "unknown" => Some(RecoveryTrigger::Unknown),
        _ => None,
    }
}

pub fn recovery_action_to_db(action: RecoveryAction) -> &'static str {
    match action {
        RecoveryAction::RetryAttempt => "retry_attempt",
        RecoveryAction::RetryWithBackoff => "retry_with_backoff",
        RecoveryAction::RestartTurn => "restart_turn",
        RecoveryAction::Fallback => "fallback",
        RecoveryAction::MarkFailed => "mark_failed",
    }
}

pub fn recovery_action_from_db(value: &str) -> Option<RecoveryAction> {
    match value {
        "retry_attempt" => Some(RecoveryAction::RetryAttempt),
        "retry_with_backoff" => Some(RecoveryAction::RetryWithBackoff),
        "restart_turn" => Some(RecoveryAction::RestartTurn),
        "fallback" => Some(RecoveryAction::Fallback),
        "mark_failed" => Some(RecoveryAction::MarkFailed),
        _ => None,
    }
}

pub fn provider_failure_class_to_db(class: ProviderFailureClass) -> &'static str {
    match class {
        ProviderFailureClass::NetworkTransient => "network_transient",
        ProviderFailureClass::RateLimit => "rate_limit",
        ProviderFailureClass::Provider5xx => "provider_5xx",
        ProviderFailureClass::AuthExpired => "auth_expired",
        ProviderFailureClass::ModelNotFound => "model_not_found",
        ProviderFailureClass::PromptTooLong => "prompt_too_long",
        ProviderFailureClass::MaxOutputTokens => "max_output_tokens",
        ProviderFailureClass::StreamStall => "stream_stall",
        ProviderFailureClass::StreamTruncated => "stream_truncated",
        ProviderFailureClass::InvalidRequest => "invalid_request",
        ProviderFailureClass::PermissionDenied => "permission_denied",
        ProviderFailureClass::Unknown => "unknown",
    }
}

pub fn provider_failure_stage_to_db(stage: ProviderFailureStage) -> &'static str {
    match stage {
        ProviderFailureStage::Connect => "connect",
        ProviderFailureStage::FirstChunk => "first_chunk",
        ProviderFailureStage::MidStream => "mid_stream",
        ProviderFailureStage::Finalize => "finalize",
    }
}

pub fn provider_failure_class_from_db(value: &str) -> Option<ProviderFailureClass> {
    match value {
        "network_transient" => Some(ProviderFailureClass::NetworkTransient),
        "rate_limit" => Some(ProviderFailureClass::RateLimit),
        "provider_5xx" => Some(ProviderFailureClass::Provider5xx),
        "auth_expired" => Some(ProviderFailureClass::AuthExpired),
        "model_not_found" => Some(ProviderFailureClass::ModelNotFound),
        "prompt_too_long" => Some(ProviderFailureClass::PromptTooLong),
        "max_output_tokens" => Some(ProviderFailureClass::MaxOutputTokens),
        "stream_stall" => Some(ProviderFailureClass::StreamStall),
        "stream_truncated" => Some(ProviderFailureClass::StreamTruncated),
        "invalid_request" => Some(ProviderFailureClass::InvalidRequest),
        "permission_denied" => Some(ProviderFailureClass::PermissionDenied),
        "unknown" => Some(ProviderFailureClass::Unknown),
        _ => None,
    }
}

pub fn provider_failure_stage_from_db(value: &str) -> Option<ProviderFailureStage> {
    match value {
        "connect" => Some(ProviderFailureStage::Connect),
        "first_chunk" => Some(ProviderFailureStage::FirstChunk),
        "mid_stream" => Some(ProviderFailureStage::MidStream),
        "finalize" => Some(ProviderFailureStage::Finalize),
        _ => None,
    }
}

pub fn prompt_manifest_profile_to_db(profile: PromptManifestProfile) -> &'static str {
    match profile {
        PromptManifestProfile::AssistantFull => "assistant_full",
        PromptManifestProfile::AssistantMinimal => "assistant_minimal",
        PromptManifestProfile::AssistantNone => "assistant_none",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        sandbox_mode_to_db, thread_mode_from_db, thread_mode_to_db, thread_status_from_db,
        thread_status_to_db, turn_status_from_db, turn_status_to_db,
    };
    use pioneer_protocol::{SandboxMode, ThreadMode, ThreadStatus, TurnStatus};

    #[test]
    fn db_values_are_snake_case() {
        assert_eq!(sandbox_mode_to_db(SandboxMode::FullAccess), "full_access");
        assert_eq!(thread_mode_to_db(ThreadMode::Chat), "chat");
        assert_eq!(thread_mode_to_db(ThreadMode::Agent), "agent");
        assert_eq!(thread_status_to_db(ThreadStatus::Active), "active");
        assert_eq!(thread_status_to_db(ThreadStatus::Idle), "idle");
        assert_eq!(thread_status_to_db(ThreadStatus::Closed), "closed");
        assert_eq!(turn_status_to_db(TurnStatus::InProgress), "in_progress");
        assert_eq!(turn_status_to_db(TurnStatus::Completed), "completed");
        assert_eq!(turn_status_to_db(TurnStatus::Failed), "failed");
        assert_eq!(turn_status_to_db(TurnStatus::Interrupted), "interrupted");
    }

    #[test]
    fn turn_status_parser_accepts_snake_case_only() {
        assert_eq!(
            turn_status_from_db("in_progress"),
            Some(TurnStatus::InProgress)
        );
        assert_eq!(
            turn_status_from_db("completed"),
            Some(TurnStatus::Completed)
        );
        assert_eq!(turn_status_from_db("failed"), Some(TurnStatus::Failed));
        assert_eq!(
            turn_status_from_db("interrupted"),
            Some(TurnStatus::Interrupted)
        );
        assert_eq!(turn_status_from_db("InProgress"), None);
    }

    #[test]
    fn thread_mode_and_status_parser_accept_snake_case_only() {
        assert_eq!(thread_mode_from_db("chat"), Some(ThreadMode::Chat));
        assert_eq!(thread_mode_from_db("agent"), Some(ThreadMode::Agent));
        assert_eq!(thread_mode_from_db("Chat"), None);

        assert_eq!(thread_status_from_db("active"), Some(ThreadStatus::Active));
        assert_eq!(thread_status_from_db("idle"), Some(ThreadStatus::Idle));
        assert_eq!(thread_status_from_db("closed"), Some(ThreadStatus::Closed));
        assert_eq!(thread_status_from_db("Active"), None);
    }
}

pub fn input_type_and_text(item: &UserInput) -> (&'static str, Option<String>) {
    match item {
        UserInput::Text { text, .. } => ("text", Some(text.clone())),
        UserInput::Image { .. } => ("image", None),
        UserInput::LocalImage { .. } => ("local_image", None),
        UserInput::File { .. } => ("file", None),
        UserInput::LocalFile { .. } => ("local_file", None),
        UserInput::Audio { .. } => ("audio", None),
        UserInput::LocalAudio { .. } => ("local_audio", None),
        UserInput::Video { .. } => ("video", None),
        UserInput::LocalVideo { .. } => ("local_video", None),
        UserInput::Artifact { .. } => ("artifact", None),
        UserInput::Skill { .. } => ("skill", None),
        UserInput::Mention { .. } => ("mention", None),
    }
}
