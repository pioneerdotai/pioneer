pub use pioneer_crud_access::turn_execution::{
    turn_kind_from_db, turn_origin_from_db, turn_status_from_db,
};

use anyhow::{Context, Result, bail};
use pioneer_protocol::{
    ExecutionWindowExhaustionReason, ExecutionWindowStatus, MemoryActorKind, MemoryCandidateStatus,
    MemoryCategory, MemoryEvidenceClass, MemoryFactClass, MemoryLifecycleActorKind,
    MemoryLifecycleReasonCode, MemoryLifetimeClass, MemoryOwnershipClass, MemoryQualityAction,
    MemoryScopeKind, MemorySensitivity, MemorySourceContextKind, MemoryStatus, MemoryWriteRelation,
    NewMemberProfile, PersistedActorRef, PromptManifestProfile, ProviderFailureClass,
    ProviderFailureStage, RecoveryAction, RecoveryJobStatus, RecoveryTrigger,
    TaskConcurrencyConflictPolicy, TaskExecutorKind, TaskOwnerKind, TaskResultCandidateStatus,
    TaskResultReviewDecision, TaskResultReviewEventKind, TaskResultReviewerKind,
    TaskRunExecutionStatus, TaskRunStatus, TaskRunThreadBindingKind, TaskRunTurnKind,
    TaskRunTurnStatus, TaskStatus, TaskTriggerKind, TaskTriggerStatus, TaskWriteLockScopeKind,
    TaskWriteLockStatus, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
    TurnAuthorSnapshot, TurnItem, TurnItemAttemptStatus, TurnItemExecutionClass,
    TurnItemTimeoutReason, TurnItemType, TurnMention, TurnPermissionMode,
    TurnPermissionProfileSource, TurnStatus, UserInput,
};
use serde::{Serialize, de::DeserializeOwned};

pub const DB_ID_LEN: usize = 21;
pub const TURN_MENTIONS_JSON_MAX_BYTES: usize = 65_536;
pub const TURN_MENTION_MAX_COUNT: usize = 64;
pub const TURN_AUTHOR_AVATAR_REVISION_MAX_BYTES: usize = 64;

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
pub const RECOVERY_STATUS_BLOCKED: &str = "blocked";
pub const RECOVERY_STATUS_CANCELLED: &str = "cancelled";

fn enum_to_snake_string<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .expect("protocol enum should serialize as a string")
}

fn enum_from_snake_string<T: DeserializeOwned>(value: &str, type_name: &str) -> Result<T> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|err| anyhow::anyhow!("unknown {type_name} `{value}`: {err}"))
}

pub use pioneer_crud_access::policy::sandbox_mode_to_db;

pub fn thread_mode_to_db(mode: ThreadMode) -> &'static str {
    match mode {
        ThreadMode::Message => "message",
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
        ThreadOriginKind::Collaborative => "collaborative",
        ThreadOriginKind::DirectMessage => "direct_message",
        ThreadOriginKind::User => "user",
        ThreadOriginKind::TaskRun => "task_run",
        ThreadOriginKind::System => "system",
    }
}

pub fn thread_origin_kind_from_db(value: &str) -> Option<ThreadOriginKind> {
    match value {
        "collaborative" => Some(ThreadOriginKind::Collaborative),
        "direct_message" => Some(ThreadOriginKind::DirectMessage),
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
        "message" => Some(ThreadMode::Message),
        "chat" => Some(ThreadMode::Chat),
        "agent" => Some(ThreadMode::Agent),
        _ => None,
    }
}

/// Read-time interpretation of a persisted Turn mode.
///
/// A pre-Epic-6 null value renders as Chat for compatibility, but it never
/// becomes eligible for Message mutations. Eligibility is derived only from
/// the raw persisted `message` discriminator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistedTurnSendMode {
    pub effective_mode: ThreadMode,
    pub message_mutation_eligible: bool,
}

pub fn turn_send_mode_to_db(mode: ThreadMode) -> &'static str {
    thread_mode_to_db(mode)
}

pub fn turn_send_mode_from_db(value: Option<&str>) -> Result<PersistedTurnSendMode> {
    match value {
        None => Ok(PersistedTurnSendMode {
            effective_mode: ThreadMode::Chat,
            message_mutation_eligible: false,
        }),
        Some("message") => Ok(PersistedTurnSendMode {
            effective_mode: ThreadMode::Message,
            message_mutation_eligible: true,
        }),
        Some("chat") => Ok(PersistedTurnSendMode {
            effective_mode: ThreadMode::Chat,
            message_mutation_eligible: false,
        }),
        Some("agent") => Ok(PersistedTurnSendMode {
            effective_mode: ThreadMode::Agent,
            message_mutation_eligible: false,
        }),
        Some(unknown) => bail!("unknown persisted Turn send mode `{unknown}`"),
    }
}

pub fn canonical_turn_mentions_json(mentions: &[TurnMention]) -> Result<String> {
    if mentions.len() > TURN_MENTION_MAX_COUNT {
        bail!("Turn mentions exceed the maximum of {TURN_MENTION_MAX_COUNT} entries");
    }

    let mut canonical = mentions.to_vec();
    canonical.sort_by(|left, right| left.principal_id.cmp(&right.principal_id));
    canonical.dedup_by(|left, right| left.principal_id == right.principal_id);
    for mention in &canonical {
        validate_turn_mention(mention)?;
    }

    let encoded = serde_json::to_string(&canonical).context("failed to encode Turn mentions")?;
    if encoded.len() > TURN_MENTIONS_JSON_MAX_BYTES {
        bail!("encoded Turn mentions exceed {TURN_MENTIONS_JSON_MAX_BYTES} bytes");
    }
    Ok(encoded)
}

pub fn turn_mentions_from_db(value: &str) -> Result<Vec<TurnMention>> {
    if value.len() > TURN_MENTIONS_JSON_MAX_BYTES {
        bail!("persisted Turn mentions exceed {TURN_MENTIONS_JSON_MAX_BYTES} bytes");
    }
    let decoded = serde_json::from_str::<Vec<TurnMention>>(value)
        .context("failed to decode persisted Turn mentions")?;
    let canonical = canonical_turn_mentions_json(&decoded)?;
    let canonical_decoded = serde_json::from_str::<Vec<TurnMention>>(&canonical)
        .context("failed to decode canonical Turn mentions")?;
    Ok(canonical_decoded)
}

pub fn validate_turn_author_snapshot(snapshot: &TurnAuthorSnapshot) -> Result<()> {
    let validated = NewMemberProfile::new(
        snapshot.display_name.clone(),
        snapshot.nickname.clone(),
        None,
    )
    .context("invalid Turn author profile snapshot")?;
    if validated.display_name != snapshot.display_name {
        bail!("Turn author display-name snapshot is not canonical");
    }
    if snapshot.avatar_revision.as_ref().is_some_and(|revision| {
        revision.is_empty()
            || revision.len() > TURN_AUTHOR_AVATAR_REVISION_MAX_BYTES
            || revision.chars().any(char::is_control)
    }) {
        bail!("invalid Turn author avatar revision snapshot");
    }
    match (&snapshot.actor, snapshot.agent.as_ref()) {
        (PersistedActorRef::AgentExecution(execution_id), Some(agent)) => {
            if &agent.agent_execution_id != execution_id
                || agent.display_name != snapshot.display_name
                || agent.nickname != snapshot.nickname
                || agent.avatar_revision != snapshot.avatar_revision
            {
                bail!("Turn agent author snapshot does not match its immutable author fields");
            }
            if agent.identity_source_revision == 0 {
                bail!("Turn agent author snapshot has a zero source revision");
            }
            pioneer_protocol::AgentDisplayName::new(agent.display_name.clone())
                .context("invalid Turn agent display-name snapshot")?;
            pioneer_protocol::AgentNicknameKey::new(agent.nickname.clone())
                .context("invalid Turn agent nickname snapshot")?;
            if let Some(role_label) = agent.role_label.as_ref() {
                pioneer_protocol::AgentRoleLabel::new(role_label.clone())
                    .context("invalid Turn agent role-label snapshot")?;
            }
        }
        (PersistedActorRef::AgentExecution(_), None) => {
            bail!("AgentExecution Turn author has no immutable presentation snapshot")
        }
        (_, Some(_)) => bail!("non-agent Turn author carries an agent presentation snapshot"),
        (_, None) => {}
    }
    Ok(())
}

fn validate_turn_mention(mention: &TurnMention) -> Result<()> {
    NewMemberProfile::new("Mention snapshot", mention.nickname.clone(), None)
        .context("invalid Turn mention nickname snapshot")?;
    Ok(())
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
        TaskStatus::WaitingReview => "waiting_review",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Blocked => "blocked",
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
        "waiting_review" => Some(TaskStatus::WaitingReview),
        "completed" => Some(TaskStatus::Completed),
        "failed" => Some(TaskStatus::Failed),
        "blocked" => Some(TaskStatus::Blocked),
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

pub use pioneer_crud_tasks::status::{task_run_status_from_db, task_run_status_to_db};

pub use pioneer_crud_tasks::status::{
    is_terminal_task_run_status, task_executor_kind_from_db, task_executor_kind_to_db,
    task_trigger_kind_from_db, task_trigger_kind_to_db, task_trigger_status_from_db,
    task_trigger_status_to_db,
};

pub fn is_terminal_task_run_status_db(status: &str) -> bool {
    task_run_status_from_db(status).is_some_and(TaskRunStatus::is_terminal)
}

pub use pioneer_crud_tasks::status::{
    is_terminal_task_run_execution_status, task_run_execution_status_from_db,
    task_run_execution_status_to_db,
};

pub fn execution_window_status_to_db(status: ExecutionWindowStatus) -> String {
    enum_to_snake_string(status)
}

pub fn execution_window_status_from_db(value: &str) -> Result<ExecutionWindowStatus> {
    enum_from_snake_string(value, "execution window status")
}

pub fn execution_window_exhaustion_reason_to_db(reason: ExecutionWindowExhaustionReason) -> String {
    enum_to_snake_string(reason)
}

pub fn execution_window_exhaustion_reason_from_db(
    value: &str,
) -> Result<ExecutionWindowExhaustionReason> {
    enum_from_snake_string(value, "execution window exhaustion reason")
}

pub fn task_run_thread_binding_kind_to_db(kind: TaskRunThreadBindingKind) -> String {
    enum_to_snake_string(kind)
}

pub fn task_run_thread_binding_kind_from_db(value: &str) -> Result<TaskRunThreadBindingKind> {
    enum_from_snake_string(value, "task run thread binding kind")
}

pub fn task_run_turn_kind_to_db(kind: TaskRunTurnKind) -> String {
    enum_to_snake_string(kind)
}

pub fn task_run_turn_kind_from_db(value: &str) -> Result<TaskRunTurnKind> {
    enum_from_snake_string(value, "task run turn kind")
}

pub fn task_run_turn_status_to_db(status: TaskRunTurnStatus) -> String {
    enum_to_snake_string(status)
}

pub fn task_run_turn_status_from_db(value: &str) -> Result<TaskRunTurnStatus> {
    enum_from_snake_string(value, "task run turn status")
}

pub fn task_result_candidate_status_to_db(status: TaskResultCandidateStatus) -> String {
    enum_to_snake_string(status)
}

pub fn task_result_candidate_status_from_db(value: &str) -> Result<TaskResultCandidateStatus> {
    enum_from_snake_string(value, "task result candidate status")
}

pub fn task_result_reviewer_kind_to_db(kind: TaskResultReviewerKind) -> String {
    enum_to_snake_string(kind)
}

pub fn task_result_reviewer_kind_from_db(value: &str) -> Result<TaskResultReviewerKind> {
    enum_from_snake_string(value, "task result reviewer kind")
}

pub fn task_result_review_event_kind_to_db(kind: TaskResultReviewEventKind) -> String {
    enum_to_snake_string(kind)
}

pub fn task_result_review_event_kind_from_db(value: &str) -> Result<TaskResultReviewEventKind> {
    enum_from_snake_string(value, "task result review event kind")
}

pub fn task_result_review_decision_to_db(decision: TaskResultReviewDecision) -> String {
    enum_to_snake_string(decision)
}

pub fn task_result_review_decision_from_db(value: &str) -> Result<TaskResultReviewDecision> {
    enum_from_snake_string(value, "task result review decision")
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
        TurnStatus::Blocked => "blocked",
    }
}

pub fn turn_kind_to_db(kind: pioneer_protocol::TurnKind) -> &'static str {
    match kind {
        pioneer_protocol::TurnKind::Conversation => "conversation",
        pioneer_protocol::TurnKind::TaskRun => "task_run",
    }
}

pub fn turn_origin_to_db(origin: pioneer_protocol::TurnOrigin) -> &'static str {
    match origin {
        pioneer_protocol::TurnOrigin::User => "user",
        pioneer_protocol::TurnOrigin::ScheduledTask => "scheduled_task",
        pioneer_protocol::TurnOrigin::DetachedTask => "detached_task",
        pioneer_protocol::TurnOrigin::AttachedTask => "attached_task",
        pioneer_protocol::TurnOrigin::TaskDelivery => "task_delivery",
    }
}

pub fn turn_permission_mode_to_db(mode: TurnPermissionMode) -> &'static str {
    match mode {
        TurnPermissionMode::FullAccess => "full_access",
        TurnPermissionMode::AutoAcceptEdits => "auto_accept_edits",
        TurnPermissionMode::Supervised => "supervised",
    }
}

pub fn turn_permission_mode_from_db(value: &str) -> Option<TurnPermissionMode> {
    match value {
        "full_access" => Some(TurnPermissionMode::FullAccess),
        "auto_accept_edits" => Some(TurnPermissionMode::AutoAcceptEdits),
        "supervised" => Some(TurnPermissionMode::Supervised),
        _ => None,
    }
}

pub fn turn_permission_profile_source_to_db(source: TurnPermissionProfileSource) -> &'static str {
    match source {
        TurnPermissionProfileSource::Composer => "composer",
        TurnPermissionProfileSource::Defaulted => "defaulted",
        TurnPermissionProfileSource::InheritedFromParentTurn => "inherited_from_parent_turn",
        TurnPermissionProfileSource::TaskPermissionCap => "task_permission_cap",
        TurnPermissionProfileSource::System => "system",
    }
}

pub fn turn_permission_profile_source_from_db(value: &str) -> Option<TurnPermissionProfileSource> {
    match value {
        "composer" => Some(TurnPermissionProfileSource::Composer),
        "defaulted" => Some(TurnPermissionProfileSource::Defaulted),
        "inherited_from_parent_turn" => Some(TurnPermissionProfileSource::InheritedFromParentTurn),
        "task_permission_cap" => Some(TurnPermissionProfileSource::TaskPermissionCap),
        "system" => Some(TurnPermissionProfileSource::System),
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

pub use pioneer_crud_conversations::turn_item::turn_item_type_from_db;

pub fn turn_item_execution_class_to_db(execution_class: TurnItemExecutionClass) -> &'static str {
    match execution_class {
        TurnItemExecutionClass::Standard => "standard",
        TurnItemExecutionClass::DurableWait => "durable_wait",
        TurnItemExecutionClass::ContextCompaction => "context_compaction",
    }
}

pub fn turn_item_execution_class_from_db(value: &str) -> Option<TurnItemExecutionClass> {
    match value {
        "standard" => Some(TurnItemExecutionClass::Standard),
        "durable_wait" => Some(TurnItemExecutionClass::DurableWait),
        "context_compaction" => Some(TurnItemExecutionClass::ContextCompaction),
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
        RecoveryJobStatus::Blocked => RECOVERY_STATUS_BLOCKED,
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
        RECOVERY_STATUS_BLOCKED => Some(RecoveryJobStatus::Blocked),
        RECOVERY_STATUS_CANCELLED => Some(RecoveryJobStatus::Cancelled),
        _ => None,
    }
}

pub fn recovery_trigger_to_db(trigger: RecoveryTrigger) -> &'static str {
    match trigger {
        RecoveryTrigger::Timeout => "timeout",
        RecoveryTrigger::ProviderError => "provider_error",
        RecoveryTrigger::TurnStart => "turn_start",
        RecoveryTrigger::TurnDispatch => "turn_dispatch",
        RecoveryTrigger::ProjectionFailure => "projection_failure",
        RecoveryTrigger::ExecutionWindowContinuation => "execution_window_continuation",
        RecoveryTrigger::ArtifactFinalization => "artifact_finalization",
        RecoveryTrigger::TaskDispatch => "task_dispatch",
        RecoveryTrigger::RuntimeFailure => "runtime_failure",
        RecoveryTrigger::Unknown => "unknown",
    }
}

pub fn recovery_trigger_from_db(value: &str) -> Option<RecoveryTrigger> {
    match value {
        "timeout" => Some(RecoveryTrigger::Timeout),
        "provider_error" => Some(RecoveryTrigger::ProviderError),
        "turn_start" => Some(RecoveryTrigger::TurnStart),
        "turn_dispatch" => Some(RecoveryTrigger::TurnDispatch),
        "projection_failure" => Some(RecoveryTrigger::ProjectionFailure),
        "execution_window_continuation" => Some(RecoveryTrigger::ExecutionWindowContinuation),
        "artifact_finalization" => Some(RecoveryTrigger::ArtifactFinalization),
        "task_dispatch" => Some(RecoveryTrigger::TaskDispatch),
        "runtime_failure" => Some(RecoveryTrigger::RuntimeFailure),
        "unknown" => Some(RecoveryTrigger::Unknown),
        _ => None,
    }
}

pub fn recovery_action_to_db(action: RecoveryAction) -> &'static str {
    match action {
        RecoveryAction::RetryAttempt => "retry_attempt",
        RecoveryAction::RetryWithBackoff => "retry_with_backoff",
        RecoveryAction::RestartTurn => "restart_turn",
        RecoveryAction::ReplayDurableEvent => "replay_durable_event",
        RecoveryAction::RehydrateTurnState => "rehydrate_turn_state",
        RecoveryAction::OpenNextExecutionWindow => "open_next_execution_window",
        RecoveryAction::AdaptProviderRequest => "adapt_provider_request",
        RecoveryAction::RefreshProviderAuth => "refresh_provider_auth",
        RecoveryAction::CompactHistory => "compact_history",
        RecoveryAction::DisableStreaming => "disable_streaming",
        RecoveryAction::DisableUnsupportedCapability => "disable_unsupported_capability",
        RecoveryAction::RepairArtifactFinalization => "repair_artifact_finalization",
        RecoveryAction::RequeueTaskDispatch => "requeue_task_dispatch",
        RecoveryAction::BlockResumable => "block_resumable",
        RecoveryAction::Fallback => "fallback",
        RecoveryAction::MarkFailed => "mark_failed",
    }
}

pub fn recovery_action_from_db(value: &str) -> Option<RecoveryAction> {
    match value {
        "retry_attempt" => Some(RecoveryAction::RetryAttempt),
        "retry_with_backoff" => Some(RecoveryAction::RetryWithBackoff),
        "restart_turn" => Some(RecoveryAction::RestartTurn),
        "replay_durable_event" => Some(RecoveryAction::ReplayDurableEvent),
        "rehydrate_turn_state" => Some(RecoveryAction::RehydrateTurnState),
        "open_next_execution_window" => Some(RecoveryAction::OpenNextExecutionWindow),
        "adapt_provider_request" => Some(RecoveryAction::AdaptProviderRequest),
        "refresh_provider_auth" => Some(RecoveryAction::RefreshProviderAuth),
        "compact_history" => Some(RecoveryAction::CompactHistory),
        "disable_streaming" => Some(RecoveryAction::DisableStreaming),
        "disable_unsupported_capability" => Some(RecoveryAction::DisableUnsupportedCapability),
        "repair_artifact_finalization" => Some(RecoveryAction::RepairArtifactFinalization),
        "requeue_task_dispatch" => Some(RecoveryAction::RequeueTaskDispatch),
        "block_resumable" => Some(RecoveryAction::BlockResumable),
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
        ProviderFailureClass::AuthOrPermission => "auth_or_permission",
        ProviderFailureClass::ModelNotFound => "model_not_found",
        ProviderFailureClass::PromptTooLong => "prompt_too_long",
        ProviderFailureClass::ContextTooLarge => "context_too_large",
        ProviderFailureClass::MaxOutputTokens => "max_output_tokens",
        ProviderFailureClass::StreamStall => "stream_stall",
        ProviderFailureClass::StreamTruncated => "stream_truncated",
        ProviderFailureClass::EmptyResponse => "empty_response",
        ProviderFailureClass::ProviderRejected => "provider_rejected",
        ProviderFailureClass::UnsupportedParameter => "unsupported_parameter",
        ProviderFailureClass::UnsupportedCapability => "unsupported_capability",
        ProviderFailureClass::UnsupportedImageInput => "unsupported_image_input",
        ProviderFailureClass::UnsupportedToolCalling => "unsupported_tool_calling",
        ProviderFailureClass::UnsupportedStreaming => "unsupported_streaming",
        ProviderFailureClass::MalformedProviderRequest => "malformed_provider_request",
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
        "auth_or_permission" => Some(ProviderFailureClass::AuthOrPermission),
        "model_not_found" => Some(ProviderFailureClass::ModelNotFound),
        "prompt_too_long" => Some(ProviderFailureClass::PromptTooLong),
        "context_too_large" => Some(ProviderFailureClass::ContextTooLarge),
        "max_output_tokens" => Some(ProviderFailureClass::MaxOutputTokens),
        "stream_stall" => Some(ProviderFailureClass::StreamStall),
        "stream_truncated" => Some(ProviderFailureClass::StreamTruncated),
        "empty_response" => Some(ProviderFailureClass::EmptyResponse),
        "provider_rejected" => Some(ProviderFailureClass::ProviderRejected),
        "unsupported_parameter" => Some(ProviderFailureClass::UnsupportedParameter),
        "unsupported_capability" => Some(ProviderFailureClass::UnsupportedCapability),
        "unsupported_image_input" => Some(ProviderFailureClass::UnsupportedImageInput),
        "unsupported_tool_calling" => Some(ProviderFailureClass::UnsupportedToolCalling),
        "unsupported_streaming" => Some(ProviderFailureClass::UnsupportedStreaming),
        "malformed_provider_request" => Some(ProviderFailureClass::MalformedProviderRequest),
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
        PromptManifestProfile::CliRuntime => "cli_runtime",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_turn_mentions_json, recovery_job_status_to_db, sandbox_mode_to_db,
        thread_mode_from_db, thread_mode_to_db, thread_status_from_db, thread_status_to_db,
        turn_mentions_from_db, turn_send_mode_from_db, turn_send_mode_to_db, turn_status_from_db,
        turn_status_to_db,
    };
    use pioneer_protocol::{
        PrincipalId, RecoveryJobStatus, SandboxMode, ThreadMode, ThreadStatus, TurnMention,
        TurnStatus,
    };

    #[test]
    fn db_values_are_snake_case() {
        assert_eq!(sandbox_mode_to_db(SandboxMode::FullAccess), "full_access");
        assert_eq!(thread_mode_to_db(ThreadMode::Message), "message");
        assert_eq!(thread_mode_to_db(ThreadMode::Chat), "chat");
        assert_eq!(thread_mode_to_db(ThreadMode::Agent), "agent");
        assert_eq!(thread_status_to_db(ThreadStatus::Active), "active");
        assert_eq!(thread_status_to_db(ThreadStatus::Idle), "idle");
        assert_eq!(thread_status_to_db(ThreadStatus::Closed), "closed");
        assert_eq!(turn_status_to_db(TurnStatus::InProgress), "in_progress");
        assert_eq!(turn_status_to_db(TurnStatus::Completed), "completed");
        assert_eq!(turn_status_to_db(TurnStatus::Failed), "failed");
        assert_eq!(turn_status_to_db(TurnStatus::Interrupted), "interrupted");
        assert_eq!(turn_status_to_db(TurnStatus::Blocked), "blocked");
        assert_eq!(
            recovery_job_status_to_db(RecoveryJobStatus::Blocked),
            "blocked"
        );
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
        assert_eq!(turn_status_from_db("blocked"), Some(TurnStatus::Blocked));
        assert_eq!(turn_status_from_db("InProgress"), None);
    }

    #[test]
    fn thread_mode_and_status_parser_accept_snake_case_only() {
        assert_eq!(thread_mode_from_db("message"), Some(ThreadMode::Message));
        assert_eq!(thread_mode_from_db("chat"), Some(ThreadMode::Chat));
        assert_eq!(thread_mode_from_db("agent"), Some(ThreadMode::Agent));
        assert_eq!(thread_mode_from_db("Chat"), None);

        assert_eq!(thread_status_from_db("active"), Some(ThreadStatus::Active));
        assert_eq!(thread_status_from_db("idle"), Some(ThreadStatus::Idle));
        assert_eq!(thread_status_from_db("closed"), Some(ThreadStatus::Closed));
        assert_eq!(thread_status_from_db("Active"), None);
    }

    #[test]
    fn persisted_turn_mode_keeps_legacy_null_history_immutable() {
        let legacy = turn_send_mode_from_db(None).expect("legacy null mode should remain readable");
        assert_eq!(legacy.effective_mode, ThreadMode::Chat);
        assert!(!legacy.message_mutation_eligible);

        let message = turn_send_mode_from_db(Some(turn_send_mode_to_db(ThreadMode::Message)))
            .expect("message mode should decode");
        assert_eq!(message.effective_mode, ThreadMode::Message);
        assert!(message.message_mutation_eligible);
        assert!(turn_send_mode_from_db(Some("Message")).is_err());
    }

    #[test]
    fn turn_mentions_are_deduplicated_and_encoded_in_principal_order() {
        let principal_a = PrincipalId::new("P00000000000000000001").unwrap();
        let principal_b = PrincipalId::new("P00000000000000000002").unwrap();
        let encoded = canonical_turn_mentions_json(&[
            TurnMention {
                principal_id: principal_b.clone(),
                nickname: "member_b".to_owned(),
            },
            TurnMention {
                principal_id: principal_a.clone(),
                nickname: "member_a".to_owned(),
            },
            TurnMention {
                principal_id: principal_b,
                nickname: "member_b".to_owned(),
            },
        ])
        .expect("mentions should encode");
        let decoded = turn_mentions_from_db(encoded.as_str()).expect("mentions should decode");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].principal_id, principal_a);
        assert_eq!(canonical_turn_mentions_json(&decoded).unwrap(), encoded);
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
        UserInput::Mention { .. } => ("mention", None),
    }
}

pub use pioneer_crud_tasks::delivery::{
    task_delivery_attempt_status_from_db, task_delivery_attempt_status_to_db,
    task_delivery_mode_from_db, task_delivery_mode_to_db, task_delivery_status_from_db,
    task_delivery_status_to_db, task_delivery_thread_target_from_db,
    task_delivery_thread_target_to_db,
};
