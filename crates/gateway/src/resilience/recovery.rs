use anyhow::{Context, Result, bail};
use futures_util::future::BoxFuture;
use pioneer_agent::{
    AgentControlError, AgentManager, ExecutionCheckpointContext, RecoveryAttemptRequest,
    RestoredRecoveryTurnRequest, RetainedProviderHistoryMessage, ToolLoopConfig,
};
use pioneer_config::GatewayCommandExecutionTimeoutConfig;
use pioneer_crud::{
    BlockedTurnRecoveryResumeOutcome, ClaimedRecoveryActivation, CrudStore,
    NewTurnExecutionCheckpointRecord, NewTurnExecutionWindowRecord, RecoveryJobRecord,
    TimeoutCandidate, TurnExecutionCheckpointKind,
};
use pioneer_protocol::{
    EXECUTION_CHECKPOINT_DEFAULT_TOOL_DETAIL_LIMIT, ExecutionCheckpointPayload,
    ExecutionCheckpointProviderBudgetInput, ExecutionCheckpointWindowSummary,
    ExecutionWindowExhaustionReason, ExecutionWindowStatus, ProviderFailureClass,
    ProviderFailureDetails, RecoveryAction, RecoveryAttemptContext, RecoveryJobStatus,
    RecoveryTrigger, ToolMetadata, ToolRecoveryIdempotencyMode, ToolRecoveryPolicySnapshot,
    ToolRecoveryRetryClass, TurnItemType, TurnStatus, UserInput,
    build_execution_checkpoint_original_request_summary, build_execution_checkpoint_payload,
    build_execution_checkpoint_provider_budget_summary, build_execution_checkpoint_tool_summary,
    generate_id,
};
use pioneer_provider::{ChatMessage, ModelInputItem, ProviderRegistry, Role};
use pioneer_tools::{ExecutionWindowAdmissionDecision, decide_execution_window_admission};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;

use super::timeout::{
    TIMEOUT_RECOVERY_SUPPRESSED_TURN_PROGRESS, TimeoutRecoveryClassification,
    classify_timeout_candidate_liveness, timeout_recovery_suppression_context,
};

const RECOVERY_JOB_CLAIM_LEASE_SECS: u64 = 45;
const ACTIVE_RECOVERY_RECHECK_SECS: i64 = 2;
const RECOVERY_ATTEMPT_ID_LEN: usize = 21;
const STREAM_TO_NON_STREAM_FALLBACK_ATTEMPT: u32 = 2;
const RECOVERY_PROGRESS_STALE_SECS: i64 = 2 * RECOVERY_JOB_CLAIM_LEASE_SECS as i64;
pub const TURN_RECOVERY_MAX_WALL_CLOCK_SECS: u64 = 15 * 60;
// This is an episode safety budget, not a universal three-minute lifetime for
// a healthy provider stream.  A fresh durable progress frontier keeps the
// attempt alive; an unchanged episode still gets bounded and re-planned.
const RECOVERY_ATTEMPT_MAX_WALL_CLOCK_SECS: u64 = 15 * 60;

/// Distinguish causal execution progress from the periodic owner heartbeat.
///
/// `item/heartbeat` is emitted by a task that is waiting for a tool/provider
/// operation and therefore can continue indefinitely even when that operation
/// is stuck. It keeps the short idle lease alive, but must not defeat the
/// bounded recovery-episode watchdog. Runtime observations are handled by the
/// explicit CLI/native liveness paths and are not a recovery progress frontier
/// by themselves.
fn is_causal_recovery_progress(activity_kind: &str) -> bool {
    !activity_kind.starts_with("runtime/") && activity_kind != "item/heartbeat"
}

type RecoveryListenerStarter =
    Arc<dyn Fn(String) -> BoxFuture<'static, std::result::Result<(), String>> + Send + Sync>;

#[derive(Debug, Clone, Copy)]
pub struct RecoveryPolicy {
    pub action: RecoveryAction,
    pub max_attempts: i64,
    pub base_backoff_secs: u64,
    pub max_wall_clock_secs: u64,
    pub no_progress_limit: i64,
}

#[derive(Debug, Clone)]
struct TimeoutRecoveryPolicyDecision {
    policy: RecoveryPolicy,
    policy_source: TimeoutRecoveryPolicySource,
    tool_snapshot: Option<ToolRecoveryPolicySnapshot>,
}

#[derive(Debug, Clone)]
struct RetainedProviderHistoryRow {
    sequence: i64,
    source: String,
    item_id: Option<String>,
    tool_name: Option<String>,
    payload: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeoutRecoveryPolicySource {
    ItemTypeRegistry,
    ToolItemSnapshot,
    ToolItemMissingSnapshot,
}

impl TimeoutRecoveryPolicySource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ItemTypeRegistry => "item_type_registry",
            Self::ToolItemSnapshot => "tool_item_snapshot",
            Self::ToolItemMissingSnapshot => "tool_item_missing_snapshot",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProviderRecoveryPolicy {
    pub action: RecoveryAction,
    pub max_attempts: i64,
    pub base_backoff_secs: u64,
    pub max_wall_clock_secs: u64,
    pub no_progress_limit: i64,
}

const fn non_retryable_provider_failure_policy() -> ProviderRecoveryPolicy {
    ProviderRecoveryPolicy {
        action: RecoveryAction::MarkFailed,
        max_attempts: 0,
        base_backoff_secs: 0,
        max_wall_clock_secs: 10,
        no_progress_limit: 0,
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryPolicyRegistry {
    by_item_type: HashMap<TurnItemType, RecoveryPolicy>,
    by_provider_failure_class: HashMap<ProviderFailureClass, ProviderRecoveryPolicy>,
}

impl Default for RecoveryPolicyRegistry {
    fn default() -> Self {
        use RecoveryAction::*;
        use TurnItemType::*;

        let mut by_item_type = HashMap::new();
        by_item_type.insert(
            UserMessage,
            RecoveryPolicy {
                action: MarkFailed,
                max_attempts: 0,
                base_backoff_secs: 0,
                max_wall_clock_secs: 10,
                no_progress_limit: 0,
            },
        );
        by_item_type.insert(
            AgentMessage,
            RecoveryPolicy {
                action: RestartTurn,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
            },
        );
        by_item_type.insert(
            Reasoning,
            RecoveryPolicy {
                action: RestartTurn,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
            },
        );
        by_item_type.insert(
            SystemEvent,
            RecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 2,
                base_backoff_secs: 1,
                max_wall_clock_secs: 90,
                no_progress_limit: 2,
            },
        );
        by_item_type.insert(
            CommandExecution,
            RecoveryPolicy {
                action: RetryAttempt,
                max_attempts: 3,
                base_backoff_secs: 2,
                max_wall_clock_secs: GatewayCommandExecutionTimeoutConfig::default()
                    .recovery_max_wall_clock_secs,
                no_progress_limit: 3,
            },
        );
        by_item_type.insert(
            FileChange,
            RecoveryPolicy {
                action: RetryAttempt,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 180,
                no_progress_limit: 2,
            },
        );
        by_item_type.insert(
            WebSearch,
            RecoveryPolicy {
                action: RetryAttempt,
                max_attempts: 3,
                base_backoff_secs: 2,
                max_wall_clock_secs: 180,
                no_progress_limit: 3,
            },
        );
        by_item_type.insert(
            WebFetch,
            RecoveryPolicy {
                action: RetryAttempt,
                max_attempts: 3,
                base_backoff_secs: 2,
                max_wall_clock_secs: 240,
                no_progress_limit: 3,
            },
        );
        by_item_type.insert(
            Download,
            RecoveryPolicy {
                action: RetryAttempt,
                max_attempts: 3,
                base_backoff_secs: 2,
                max_wall_clock_secs: 600,
                no_progress_limit: 3,
            },
        );
        by_item_type.insert(
            DynamicToolCall,
            RecoveryPolicy {
                action: RetryAttempt,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 300,
                no_progress_limit: 2,
            },
        );

        let mut by_provider_failure_class = HashMap::new();
        by_provider_failure_class.insert(
            ProviderFailureClass::NetworkTransient,
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 3,
                base_backoff_secs: 2,
                max_wall_clock_secs: TURN_RECOVERY_MAX_WALL_CLOCK_SECS,
                no_progress_limit: 3,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::RateLimit,
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 4,
                base_backoff_secs: 3,
                max_wall_clock_secs: TURN_RECOVERY_MAX_WALL_CLOCK_SECS,
                no_progress_limit: 4,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::Provider5xx,
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 3,
                base_backoff_secs: 2,
                max_wall_clock_secs: TURN_RECOVERY_MAX_WALL_CLOCK_SECS,
                no_progress_limit: 3,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::AuthExpired,
            ProviderRecoveryPolicy {
                action: RefreshProviderAuth,
                max_attempts: 1,
                base_backoff_secs: 2,
                max_wall_clock_secs: 120,
                no_progress_limit: 1,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::AuthOrPermission,
            ProviderRecoveryPolicy {
                action: RefreshProviderAuth,
                max_attempts: 1,
                base_backoff_secs: 2,
                max_wall_clock_secs: 60,
                no_progress_limit: 1,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::ModelNotFound,
            ProviderRecoveryPolicy {
                action: BlockResumable,
                max_attempts: 0,
                base_backoff_secs: 0,
                max_wall_clock_secs: 10,
                no_progress_limit: 0,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::PromptTooLong,
            ProviderRecoveryPolicy {
                action: CompactHistory,
                max_attempts: 1,
                base_backoff_secs: 1,
                max_wall_clock_secs: 120,
                no_progress_limit: 1,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::ContextTooLarge,
            ProviderRecoveryPolicy {
                action: CompactHistory,
                max_attempts: 1,
                base_backoff_secs: 1,
                max_wall_clock_secs: 120,
                no_progress_limit: 1,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::UnsupportedStreaming,
            ProviderRecoveryPolicy {
                action: DisableStreaming,
                max_attempts: 1,
                base_backoff_secs: 1,
                max_wall_clock_secs: 120,
                no_progress_limit: 1,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::UnsupportedParameter,
            non_retryable_provider_failure_policy(),
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::UnsupportedCapability,
            non_retryable_provider_failure_policy(),
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::UnsupportedImageInput,
            ProviderRecoveryPolicy {
                action: DisableUnsupportedCapability,
                max_attempts: 1,
                base_backoff_secs: 1,
                max_wall_clock_secs: 60,
                no_progress_limit: 1,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::UnsupportedToolCalling,
            ProviderRecoveryPolicy {
                action: DisableUnsupportedCapability,
                max_attempts: 1,
                base_backoff_secs: 1,
                max_wall_clock_secs: 60,
                no_progress_limit: 1,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::MalformedProviderRequest,
            non_retryable_provider_failure_policy(),
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::MaxOutputTokens,
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 2,
                base_backoff_secs: 1,
                max_wall_clock_secs: 90,
                no_progress_limit: 2,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::StreamStall,
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 600,
                no_progress_limit: 2,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::StreamTruncated,
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 600,
                no_progress_limit: 2,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::EmptyResponse,
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 300,
                no_progress_limit: 2,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::ProviderRejected,
            non_retryable_provider_failure_policy(),
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::InvalidRequest,
            non_retryable_provider_failure_policy(),
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::PermissionDenied,
            ProviderRecoveryPolicy {
                action: RefreshProviderAuth,
                max_attempts: 1,
                base_backoff_secs: 2,
                max_wall_clock_secs: 60,
                no_progress_limit: 1,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::Unknown,
            non_retryable_provider_failure_policy(),
        );

        Self {
            by_item_type,
            by_provider_failure_class,
        }
    }
}

impl RecoveryPolicyRegistry {
    pub fn with_command_execution_timeout_config(
        command_execution_config: GatewayCommandExecutionTimeoutConfig,
    ) -> Self {
        let mut registry = Self::default();
        if let Some(policy) = registry
            .by_item_type
            .get_mut(&TurnItemType::CommandExecution)
        {
            policy.max_wall_clock_secs = command_execution_config.recovery_max_wall_clock_secs;
        }
        registry
    }
}

impl RecoveryPolicyRegistry {
    pub fn policy_for_item_type(&self, item_type: TurnItemType) -> RecoveryPolicy {
        self.by_item_type
            .get(&item_type)
            .copied()
            .unwrap_or(RecoveryPolicy {
                action: RecoveryAction::RetryAttempt,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
            })
    }

    pub fn policy_for_provider_failure(
        &self,
        class: ProviderFailureClass,
    ) -> ProviderRecoveryPolicy {
        self.by_provider_failure_class
            .get(&class)
            .copied()
            .unwrap_or_else(non_retryable_provider_failure_policy)
    }
}

#[derive(Debug, Clone)]
pub struct ProviderFailureCandidate {
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub failure: ProviderFailureDetails,
}

#[derive(Debug, Clone)]
pub struct RuntimeFailureCandidate {
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub trigger: RecoveryTrigger,
    pub action: RecoveryAction,
    pub reason: String,
    pub base_backoff_secs: u64,
    pub max_attempts: i64,
    pub max_wall_clock_secs: u64,
    pub no_progress_limit: i64,
    pub metadata: ToolMetadata,
}

#[derive(Clone)]
pub struct RecoveryCoordinator {
    crud_store: Arc<CrudStore>,
    agent_manager: Arc<AgentManager>,
    policy_registry: RecoveryPolicyRegistry,
    tool_loop_config: ToolLoopConfig,
    authorization_invalidation_hub: Arc<crate::authorization::AuthorizationInvalidationHub>,
    execution_leases: Arc<crate::authorization::ExecutionLeaseRegistry>,
    listener_starter: Arc<RwLock<Option<RecoveryListenerStarter>>>,
}

#[derive(Debug, Clone)]
pub struct RecoveryTerminalOutcome {
    pub job_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub attempt_number: u32,
    pub status: RecoveryJobStatus,
    pub error_message: String,
}

#[derive(Debug, Clone)]
pub enum RecoveryJobEnqueueOutcome {
    Created(RecoveryJobRecord),
    Reused(RecoveryJobRecord),
}

impl RecoveryJobEnqueueOutcome {
    pub fn job(&self) -> &RecoveryJobRecord {
        match self {
            Self::Created(job) | Self::Reused(job) => job,
        }
    }

    pub fn into_job(self) -> RecoveryJobRecord {
        match self {
            Self::Created(job) | Self::Reused(job) => job,
        }
    }

    pub fn is_created(&self) -> bool {
        matches!(self, Self::Created(_))
    }

    pub fn next_attempt_number(&self) -> u32 {
        attempt_number_for_job(self.job())
    }
}

#[derive(Debug, Clone)]
pub enum RecoveryCoordinatorEvent {
    RecoveryOpened {
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        attempt_number: u32,
    },
    RecoveryAttached {
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        recovery_item_id: String,
        recovery_item_type: TurnItemType,
        trigger: RecoveryTrigger,
        action: RecoveryAction,
        existing_status: RecoveryJobStatus,
        next_attempt_number: u32,
    },
    RetryScheduled {
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        attempt_number: u32,
        next_run_at_unix: i64,
        reason: Option<String>,
    },
    RetryAttemptStarted {
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        attempt_number: u32,
    },
    CliRuntimeRetryAttemptRequested(Box<CliRuntimeRecoveryAttemptRequest>),
    CliRuntimeTerminalReconciliationRequested(Box<CliRuntimeTerminalReconciliationRequest>),
    RecoverySucceeded {
        job_id: String,
        turn_id: String,
        item_id: String,
        item_type: TurnItemType,
        attempt_number: u32,
    },
    RecoveryBlocked {
        job_id: String,
        turn_id: String,
        reason: String,
    },
    RecoveryExhausted(RecoveryTerminalOutcome),
}

#[derive(Debug, Clone)]
pub struct CliRuntimeRecoveryAttemptRequest {
    pub job_id: String,
    pub recovery_attempt_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub attempt_number: u32,
    pub execution_window_index: u32,
    pub previous_failure_reason: String,
    pub binding: pioneer_crud::CliRuntimeTurnBindingRecord,
}

#[derive(Debug, Clone)]
pub struct CliRuntimeTerminalReconciliationRequest {
    pub job_id: String,
    pub recovery_attempt_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: TurnItemType,
    pub attempt_number: u32,
    pub binding: pioneer_crud::CliRuntimeTurnBindingRecord,
}

#[derive(Debug, Default, Clone)]
struct RecoveryAttemptPlan {
    force_non_stream: bool,
    disable_tool_calling: bool,
    disable_image_input: bool,
    refresh_provider_auth: bool,
    compact_history: bool,
    continue_generation: bool,
    model_override: Option<String>,
    terminal_reason: Option<String>,
}

#[derive(Debug)]
enum RestoredRecoveryTurnRequestLookup {
    Available(RestoredRecoveryTurnRequest),
    Unavailable(RestoredRecoveryTurnUnavailable),
}

#[cfg(test)]
impl RestoredRecoveryTurnRequestLookup {
    fn into_available(self) -> Option<RestoredRecoveryTurnRequest> {
        match self {
            Self::Available(request) => Some(request),
            Self::Unavailable(_) => None,
        }
    }
}

#[derive(Debug)]
enum RestoredRecoveryTurnUnavailable {
    TurnNotFound,
    TurnNotInProgress,
    MissingRuntimeSnapshot,
    MissingExecutionSecuritySnapshot,
    SnapshotMismatch,
    SnapshotInvalid { error: String },
    ExecutionWindowContinuationBlocked { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExecutionWindowContinuationAdmission {
    Open { window_index: u32 },
    Block { total_windows: u32, reason: String },
}

impl RestoredRecoveryTurnUnavailable {
    fn lost_loop_block_reason(&self) -> Option<String> {
        match self {
            Self::MissingRuntimeSnapshot => Some(
                "cannot restore recovery after agent loop loss because durable turn runtime snapshot is missing"
                    .to_owned(),
            ),
            Self::MissingExecutionSecuritySnapshot => Some(
                "cannot restore recovery after agent loop loss because durable turn execution security snapshot is missing"
                    .to_owned(),
            ),
            Self::SnapshotMismatch => Some(
                "cannot restore recovery after agent loop loss because durable turn runtime snapshot does not match the turn"
                    .to_owned(),
            ),
            Self::SnapshotInvalid { error } => Some(format!(
                "cannot restore recovery after agent loop loss because durable turn runtime snapshot is invalid: {error}"
            )),
            Self::ExecutionWindowContinuationBlocked { reason } => Some(reason.clone()),
            Self::TurnNotFound | Self::TurnNotInProgress => None,
        }
    }
}

fn execution_window_limit_reason(total_windows: u32, max_windows_per_turn: u32) -> String {
    format!(
        "max execution windows per turn reached: limit={max_windows_per_turn}, observed={total_windows}"
    )
}

fn execution_window_no_progress_reason(limit: u32, observed: u32) -> String {
    format!(
        "max_consecutive_no_progress_windows reached: limit={limit}, observed={observed}; automatic recovery stopped because consecutive execution windows produced no durable agent round or tool result"
    )
}

impl RecoveryCoordinator {
    pub fn new(
        crud_store: Arc<CrudStore>,
        agent_manager: Arc<AgentManager>,
        _provider_registry: Arc<ProviderRegistry>,
        policy_registry: RecoveryPolicyRegistry,
        tool_loop_config: ToolLoopConfig,
    ) -> Self {
        let authorization_invalidation_hub = Arc::new(
            crate::authorization::AuthorizationInvalidationHub::durable(crud_store.clone()),
        );
        Self {
            crud_store,
            agent_manager,
            policy_registry,
            tool_loop_config,
            authorization_invalidation_hub,
            execution_leases: Arc::new(crate::authorization::ExecutionLeaseRegistry::default()),
            listener_starter: Arc::new(RwLock::new(None)),
        }
    }

    pub(crate) async fn set_listener_starter(&self, starter: RecoveryListenerStarter) {
        *self.listener_starter.write().await = Some(starter);
    }

    async fn ensure_recovery_listener(&self, thread_id: &str) -> Result<(), AgentControlError> {
        let starter = self.listener_starter.read().await.clone().ok_or_else(|| {
            AgentControlError::Internal(
                "native recovery durable listener is not configured".to_owned(),
            )
        })?;
        starter(thread_id.to_owned())
            .await
            .map_err(AgentControlError::Internal)
    }

    pub(crate) fn with_authorization_invalidation_hub(
        mut self,
        hub: Arc<crate::authorization::AuthorizationInvalidationHub>,
    ) -> Self {
        self.authorization_invalidation_hub = hub;
        self
    }

    pub(crate) fn with_execution_leases(
        mut self,
        execution_leases: Arc<crate::authorization::ExecutionLeaseRegistry>,
    ) -> Self {
        self.execution_leases = execution_leases;
        self
    }

    fn provider_recovery_policy(&self, failure: &ProviderFailureDetails) -> ProviderRecoveryPolicy {
        if !failure.is_recoverable_hint {
            return non_retryable_provider_failure_policy();
        }
        self.policy_registry
            .policy_for_provider_failure(failure.class)
    }

    pub async fn enqueue_timeout_job(
        &self,
        candidate: &TimeoutCandidate,
        now_unix: i64,
    ) -> Result<RecoveryJobEnqueueOutcome> {
        if let Some(existing) = self
            .open_recovery_for_turn(candidate.turn_id.as_str())
            .await?
        {
            let _ = self
                .crud_store
                .mark_attempt_recovery_action(
                    candidate.attempt_id.as_str(),
                    existing.action,
                    now_unix,
                )
                .await?;
            return Ok(RecoveryJobEnqueueOutcome::Reused(existing));
        }

        let decision = self.policy_for_timeout_candidate(candidate).await?;
        let policy = decision.policy;

        let mut snapshot = serde_json::json!({
            "source": "timeout",
            "policy_source": decision.policy_source.as_str(),
            "item_type": format!("{:?}", candidate.item_type),
            "timeout_reason": format!("{:?}", candidate.timeout_reason),
            "action": policy_action_name(policy.action),
            "base_backoff_secs": policy.base_backoff_secs,
            "max_attempts": policy.max_attempts,
            "max_wall_clock_secs": policy.max_wall_clock_secs,
            "no_progress_limit": policy.no_progress_limit,
        });

        if let Some(tool_snapshot) = decision.tool_snapshot {
            snapshot["retry_class"] =
                serde_json::json!(tool_retry_class_name(tool_snapshot.retry_class));
            snapshot["idempotency_mode"] =
                serde_json::json!(tool_idempotency_mode_name(tool_snapshot.idempotency_mode));
            snapshot["can_resume"] = serde_json::json!(tool_snapshot.can_resume);
            snapshot["tool_recovery_policy"] =
                serde_json::to_value(tool_snapshot).unwrap_or_else(|_| serde_json::json!({}));
        }

        let record = self
            .crud_store
            .enqueue_recovery_job(
                candidate.turn_id.clone(),
                candidate.item_id.clone(),
                candidate.item_type,
                Some(candidate.attempt_id.clone()),
                RecoveryTrigger::Timeout,
                policy.action,
                Some(format!(
                    "attempt {} timed out with {:?}",
                    candidate.attempt_number, candidate.timeout_reason
                )),
                None,
                None,
                None,
                0,
                policy.max_attempts,
                snapshot.clone(),
                snapshot,
                now_unix,
            )
            .await?;

        let _ = self
            .crud_store
            .mark_attempt_recovery_action(candidate.attempt_id.as_str(), policy.action, now_unix)
            .await?;

        Ok(RecoveryJobEnqueueOutcome::Created(record))
    }

    pub async fn enqueue_provider_failure_job(
        &self,
        candidate: &ProviderFailureCandidate,
        now_unix: i64,
    ) -> Result<RecoveryJobEnqueueOutcome> {
        if let Some(existing) = self
            .open_recovery_for_turn(candidate.turn_id.as_str())
            .await?
        {
            return Ok(RecoveryJobEnqueueOutcome::Reused(existing));
        }

        let provider_policy = self.provider_recovery_policy(&candidate.failure);

        // TODO(fallback_model): persist configured fallback model into provider snapshot,
        // so model-not-found recovery can use deterministic provider-specific fallback first.
        let snapshot = serde_json::json!({
            "source": "provider_error",
            "provider": candidate.failure.provider,
            "model": candidate.failure.model,
            "transport": candidate.failure.transport,
            "class": candidate.failure.class,
            "stage": candidate.failure.stage,
            "base_backoff_secs": provider_policy.base_backoff_secs,
            "max_attempts": provider_policy.max_attempts,
            "max_wall_clock_secs": provider_policy.max_wall_clock_secs,
            "no_progress_limit": provider_policy.no_progress_limit,
            "is_recoverable_hint": candidate.failure.is_recoverable_hint,
        });

        let record = self
            .crud_store
            .enqueue_recovery_job(
                candidate.turn_id.clone(),
                candidate.item_id.clone(),
                candidate.item_type,
                None,
                RecoveryTrigger::ProviderError,
                provider_policy.action,
                candidate.failure.message.clone(),
                Some(candidate.failure.class),
                Some(candidate.failure.stage),
                candidate
                    .failure
                    .retry_after_ms
                    .map(|value| i64::try_from(value).unwrap_or(i64::MAX)),
                0,
                provider_policy.max_attempts,
                snapshot.clone(),
                snapshot,
                now_unix,
            )
            .await?;
        Ok(RecoveryJobEnqueueOutcome::Created(record))
    }

    pub async fn enqueue_runtime_failure_job(
        &self,
        candidate: &RuntimeFailureCandidate,
        now_unix: i64,
    ) -> Result<RecoveryJobEnqueueOutcome> {
        if let Some(existing) = self
            .open_recovery_for_turn(candidate.turn_id.as_str())
            .await?
        {
            return Ok(RecoveryJobEnqueueOutcome::Reused(existing));
        }

        let mut snapshot = serde_json::json!({
            "source": "runtime_failure",
            "trigger": recovery_trigger_name(candidate.trigger),
            "action": policy_action_name(candidate.action),
            "base_backoff_secs": candidate.base_backoff_secs,
            "max_attempts": candidate.max_attempts,
            "max_wall_clock_secs": candidate.max_wall_clock_secs,
            "no_progress_limit": candidate.no_progress_limit,
        });

        if let serde_json::Value::Object(snapshot_object) = &mut snapshot {
            snapshot_object.insert("metadata".to_owned(), candidate.metadata.to_json());
        }

        let record = self
            .crud_store
            .enqueue_recovery_job(
                candidate.turn_id.clone(),
                candidate.item_id.clone(),
                candidate.item_type,
                None,
                candidate.trigger,
                candidate.action,
                Some(candidate.reason.clone()),
                None,
                None,
                None,
                0,
                candidate.max_attempts,
                snapshot.clone(),
                snapshot,
                now_unix,
            )
            .await?;
        Ok(RecoveryJobEnqueueOutcome::Created(record))
    }

    pub async fn record_recovery_provider_failure(
        &self,
        recovery_job_id: &str,
        recovery_attempt_id: &str,
        failure: ProviderFailureDetails,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let Some(job) = self.crud_store.get_recovery_job(recovery_job_id).await? else {
            return Ok(Vec::new());
        };

        if job.status != RecoveryJobStatus::Active
            || job.active_attempt_id.as_deref() != Some(recovery_attempt_id)
        {
            return Ok(Vec::new());
        }

        if self.provider_recovery_policy(&failure).action == RecoveryAction::MarkFailed {
            return self
                .terminalize_active_non_retryable_provider_failure(
                    job,
                    recovery_attempt_id,
                    failure,
                    now_unix,
                )
                .await;
        }

        self.record_active_recovery_failure(
            job,
            recovery_attempt_id,
            failure.message,
            failure.retry_after_ms,
            now_unix,
        )
        .await
    }

    async fn terminalize_active_non_retryable_provider_failure(
        &self,
        job: RecoveryJobRecord,
        recovery_attempt_id: &str,
        failure: ProviderFailureDetails,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let attempt_number = attempt_number_for_job(&job);
        let detail = failure
            .message
            .unwrap_or_else(|| "provider rejected the request".to_owned());
        let message = format!(
            "non-retryable provider request ({:?}); unchanged request will not be sent again: {detail}",
            failure.class
        );

        if !self
            .crud_store
            .mark_recovery_job_terminal_after_attempt(
                job.id.as_str(),
                recovery_attempt_id,
                RecoveryJobStatus::Failed,
                Some(message.clone()),
                now_unix,
            )
            .await?
        {
            return Ok(Vec::new());
        }

        self.cancel_other_open_jobs_after_terminal_recovery(
            job.turn_id.as_str(),
            job.id.as_str(),
            now_unix,
        )
        .await?;

        Ok(vec![RecoveryCoordinatorEvent::RecoveryExhausted(
            RecoveryTerminalOutcome {
                job_id: job.id,
                turn_id: job.turn_id,
                item_id: job.item_id,
                item_type: job.item_type,
                attempt_number,
                status: RecoveryJobStatus::Failed,
                error_message: message,
            },
        )])
    }

    pub async fn record_cli_runtime_attempt_failure(
        &self,
        recovery_job_id: &str,
        recovery_attempt_id: &str,
        failure_message: String,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let Some(job) = self.crud_store.get_recovery_job(recovery_job_id).await? else {
            return Ok(Vec::new());
        };
        if job.status != RecoveryJobStatus::Active
            || job.active_attempt_id.as_deref() != Some(recovery_attempt_id)
        {
            return Ok(Vec::new());
        }

        self.record_active_recovery_failure(
            job,
            recovery_attempt_id,
            Some(failure_message),
            None,
            now_unix,
        )
        .await
    }

    pub async fn record_recovery_timeout_failure(
        &self,
        candidate: &TimeoutCandidate,
        now_unix: i64,
    ) -> Result<Option<(String, Vec<RecoveryCoordinatorEvent>)>> {
        let jobs = self
            .crud_store
            .find_recovery_jobs_by_turn_and_status(
                candidate.turn_id.as_str(),
                RecoveryJobStatus::Active,
            )
            .await?;
        if jobs.is_empty() {
            return Ok(None);
        }

        if jobs[0].active_attempt_id.is_none() {
            return Ok(None);
        }
        let recovery_job_id = jobs[0].id.clone();
        let _ = self
            .crud_store
            .mark_attempt_recovery_action(candidate.attempt_id.as_str(), jobs[0].action, now_unix)
            .await?;

        let mut events = Vec::new();
        let message = Some(format!(
            "recovery attempt timed out with {:?}",
            candidate.timeout_reason
        ));
        for job in jobs {
            let Some(job_active_attempt_id) = job.active_attempt_id.clone() else {
                continue;
            };
            events.extend(
                self.record_active_recovery_failure(
                    job,
                    job_active_attempt_id.as_str(),
                    message.clone(),
                    None,
                    now_unix,
                )
                .await?,
            );
        }

        Ok(Some((recovery_job_id, events)))
    }

    async fn record_active_recovery_failure(
        &self,
        job: RecoveryJobRecord,
        active_attempt_id: &str,
        failure_message: Option<String>,
        retry_after_ms: Option<u64>,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let policy = self.policy_for_recovery_job(&job).await?;
        let attempt_number = attempt_number_for_job(&job);
        let consumed_run_count = job.run_count.saturating_add(1);
        let wall_clock_exceeded = i64::try_from(policy.max_wall_clock_secs)
            .ok()
            .is_some_and(|limit| now_unix.saturating_sub(job.scheduled_at_unix) > limit);
        let no_progress_exceeded =
            policy.no_progress_limit >= 0 && consumed_run_count >= policy.no_progress_limit;
        let attempts_exhausted =
            policy.max_attempts >= 0 && consumed_run_count >= policy.max_attempts;

        if wall_clock_exceeded || no_progress_exceeded || attempts_exhausted {
            let message = if wall_clock_exceeded {
                "recovery wall-clock budget exhausted".to_owned()
            } else if attempts_exhausted {
                "recovery attempts exhausted".to_owned()
            } else {
                "recovery no-progress guardrail exhausted".to_owned()
            };

            let last_error = failure_message
                .clone()
                .map(|detail| format!("{message}: {detail}"))
                .unwrap_or_else(|| message.clone());

            if self
                .crud_store
                .mark_recovery_job_terminal_after_attempt(
                    job.id.as_str(),
                    active_attempt_id,
                    RecoveryJobStatus::Exhausted,
                    Some(last_error),
                    now_unix,
                )
                .await?
            {
                self.cancel_other_open_jobs_after_terminal_recovery(
                    job.turn_id.as_str(),
                    job.id.as_str(),
                    now_unix,
                )
                .await?;
                return Ok(vec![RecoveryCoordinatorEvent::RecoveryExhausted(
                    RecoveryTerminalOutcome {
                        job_id: job.id,
                        turn_id: job.turn_id,
                        item_id: job.item_id,
                        item_type: job.item_type,
                        attempt_number,
                        status: RecoveryJobStatus::Exhausted,
                        error_message: message,
                    },
                )]);
            }

            return Ok(Vec::new());
        }

        let next_run_at_unix = backoff_deadline(
            now_unix,
            policy.base_backoff_secs,
            job.run_count,
            job.id.as_str(),
            retry_after_ms
                .map(|value| i64::try_from(value).unwrap_or(i64::MAX))
                .or(job.retry_after_ms),
        );
        let reason = failure_message;
        if self
            .crud_store
            .mark_recovery_job_retrying(
                job.id.as_str(),
                active_attempt_id,
                next_run_at_unix,
                reason.clone(),
                now_unix,
            )
            .await?
        {
            let next_attempt_number = attempt_number.saturating_add(1);
            return Ok(vec![RecoveryCoordinatorEvent::RetryScheduled {
                job_id: job.id,
                turn_id: job.turn_id,
                item_id: job.item_id,
                item_type: job.item_type,
                attempt_number: next_attempt_number,
                next_run_at_unix,
                reason,
            }]);
        }

        Ok(Vec::new())
    }

    pub async fn complete_active_recovery_for_turn(
        &self,
        turn_id: &str,
        recovery: Option<&RecoveryAttemptContext>,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        self.complete_active_recovery_with_cancel_reason(
            turn_id,
            recovery,
            now_unix,
            "turn finished; pending recovery jobs cancelled",
        )
        .await
    }

    async fn complete_active_recovery_with_cancel_reason(
        &self,
        turn_id: &str,
        recovery: Option<&RecoveryAttemptContext>,
        now_unix: i64,
        cancel_reason: &str,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let jobs = if let Some(recovery) = recovery {
            self.crud_store
                .get_recovery_job(recovery.job_id.as_str())
                .await?
                .filter(|job| job.turn_id == turn_id)
                .filter(|job| job.status == RecoveryJobStatus::Active)
                .filter(|job| {
                    job.active_attempt_id.as_deref() == Some(recovery.attempt_id.as_str())
                })
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let mut events = Vec::new();
        let mut completed_job_id = None;

        for job in jobs {
            let Some(active_attempt_id) = job.active_attempt_id.clone() else {
                continue;
            };
            let attempt_number = attempt_number_for_job(&job);
            if self
                .crud_store
                .mark_recovery_job_terminal_after_attempt(
                    job.id.as_str(),
                    active_attempt_id.as_str(),
                    RecoveryJobStatus::Succeeded,
                    None,
                    now_unix,
                )
                .await?
            {
                completed_job_id = Some(job.id.clone());
                events.push(RecoveryCoordinatorEvent::RecoverySucceeded {
                    job_id: job.id,
                    turn_id: job.turn_id,
                    item_id: job.item_id,
                    item_type: job.item_type,
                    attempt_number,
                });
            }
        }

        if recovery.is_none() || completed_job_id.is_some() {
            let _ = self
                .crud_store
                .cancel_open_recovery_jobs_for_turn(
                    turn_id,
                    completed_job_id.as_deref(),
                    Some(cancel_reason.to_owned()),
                    now_unix,
                )
                .await?;
        }

        Ok(events)
    }

    pub async fn succeed_active_recovery_attempt(
        &self,
        turn_id: &str,
        recovery: &RecoveryAttemptContext,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        self.complete_active_recovery_with_cancel_reason(
            turn_id,
            Some(recovery),
            now_unix,
            "recovery attempt succeeded; pending recovery jobs cancelled",
        )
        .await
    }

    pub async fn fail_active_recoveries_for_turn(
        &self,
        turn_id: &str,
        recovery: Option<&RecoveryAttemptContext>,
        error_message: &str,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        self.fail_active_recoveries_for_turn_with_cancel_reason(
            turn_id,
            recovery,
            error_message,
            now_unix,
            "turn failed; pending recovery jobs cancelled",
        )
        .await
    }

    pub async fn block_active_recoveries_for_turn(
        &self,
        turn_id: &str,
        recovery: Option<&RecoveryAttemptContext>,
        error_message: &str,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let jobs = if let Some(recovery) = recovery {
            self.crud_store
                .get_recovery_job(recovery.job_id.as_str())
                .await?
                .filter(|job| job.turn_id == turn_id)
                .filter(|job| job.status == RecoveryJobStatus::Active)
                .filter(|job| {
                    job.active_attempt_id.as_deref() == Some(recovery.attempt_id.as_str())
                })
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            self.crud_store
                .find_recovery_jobs_by_turn_and_status(turn_id, RecoveryJobStatus::Active)
                .await?
        };

        let mut blocked_any = false;
        for job in jobs {
            if let Some(active_attempt_id) = job.active_attempt_id.clone() {
                blocked_any |= self
                    .crud_store
                    .mark_recovery_job_terminal_after_attempt(
                        job.id.as_str(),
                        active_attempt_id.as_str(),
                        RecoveryJobStatus::Blocked,
                        Some(error_message.to_owned()),
                        now_unix,
                    )
                    .await?;
            } else {
                blocked_any |= self
                    .crud_store
                    .mark_malformed_active_recovery_job_terminal(
                        job.id.as_str(),
                        RecoveryJobStatus::Blocked,
                        Some(error_message.to_owned()),
                        now_unix,
                    )
                    .await?;
            }
        }

        if recovery.is_none() || blocked_any {
            let _ = self
                .crud_store
                .cancel_open_recovery_jobs_for_turn(
                    turn_id,
                    None,
                    Some("turn blocked; pending recovery jobs cancelled".to_owned()),
                    now_unix,
                )
                .await?;
        }

        Ok(Vec::new())
    }

    async fn fail_active_recoveries_for_turn_with_cancel_reason(
        &self,
        turn_id: &str,
        recovery: Option<&RecoveryAttemptContext>,
        error_message: &str,
        now_unix: i64,
        pending_cancel_reason: &str,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let jobs = if let Some(recovery) = recovery {
            self.crud_store
                .get_recovery_job(recovery.job_id.as_str())
                .await?
                .filter(|job| job.turn_id == turn_id)
                .filter(|job| job.status == RecoveryJobStatus::Active)
                .filter(|job| {
                    job.active_attempt_id.as_deref() == Some(recovery.attempt_id.as_str())
                })
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let mut events = Vec::new();
        let mut failed_job_id = None;

        for job in jobs {
            let Some(active_attempt_id) = job.active_attempt_id.clone() else {
                continue;
            };
            let attempt_number = attempt_number_for_job(&job);
            if self
                .crud_store
                .mark_recovery_job_terminal_after_attempt(
                    job.id.as_str(),
                    active_attempt_id.as_str(),
                    RecoveryJobStatus::Failed,
                    Some(error_message.to_owned()),
                    now_unix,
                )
                .await?
            {
                failed_job_id = Some(job.id.clone());
                events.push(RecoveryCoordinatorEvent::RecoveryExhausted(
                    RecoveryTerminalOutcome {
                        job_id: job.id,
                        turn_id: job.turn_id,
                        item_id: job.item_id,
                        item_type: job.item_type,
                        attempt_number,
                        status: RecoveryJobStatus::Failed,
                        error_message: error_message.to_owned(),
                    },
                ));
            }
        }

        if recovery.is_none() || failed_job_id.is_some() {
            let _ = self
                .crud_store
                .cancel_open_recovery_jobs_for_turn(
                    turn_id,
                    failed_job_id.as_deref(),
                    Some(pending_cancel_reason.to_owned()),
                    now_unix,
                )
                .await?;
        }

        Ok(events)
    }

    pub async fn is_active_recovery_attempt(
        &self,
        turn_id: &str,
        recovery: &RecoveryAttemptContext,
    ) -> Result<bool> {
        Ok(self
            .crud_store
            .get_recovery_job(recovery.job_id.as_str())
            .await?
            .is_some_and(|job| {
                job.turn_id == turn_id
                    && job.status == RecoveryJobStatus::Active
                    && job.active_attempt_id.as_deref() == Some(recovery.attempt_id.as_str())
            }))
    }

    pub async fn run_ready_jobs(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let mut events = Vec::new();
        let mut phase_errors = Vec::new();

        match self.reconcile_orphan_native_turns(now_unix, limit).await {
            Ok(()) => {}
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "recovery coordinator orphan-native-turn phase failed"
                );
                phase_errors.push(format!("orphan native turns: {error:#}"));
            }
        }

        match self.backfill_timeout_jobs(now_unix, limit).await {
            Ok(mut phase_events) => events.append(&mut phase_events),
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "recovery coordinator timeout backfill phase failed"
                );
                phase_errors.push(format!("timeout backfill: {error:#}"));
            }
        }

        match self.expire_active_recovery_jobs(now_unix, limit).await {
            Ok(mut phase_events) => events.append(&mut phase_events),
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "recovery coordinator active expiration phase failed"
                );
                phase_errors.push(format!("active expiration: {error:#}"));
            }
        }

        match self
            .repair_due_terminal_recovery_jobs(now_unix, limit)
            .await
        {
            Ok(mut phase_events) => events.append(&mut phase_events),
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "recovery coordinator terminal repair phase failed"
                );
                phase_errors.push(format!("terminal repair: {error:#}"));
            }
        }

        match self
            .crud_store
            .claim_due_recovery_jobs(now_unix, RECOVERY_JOB_CLAIM_LEASE_SECS, limit)
            .await
        {
            Ok(jobs) => {
                for job in jobs {
                    match self.run_single_job(job, now_unix).await {
                        Ok(mut job_events) => events.append(&mut job_events),
                        Err(error) => {
                            warn!(
                                error = %format!("{error:#}"),
                                "recovery coordinator due job phase failed"
                            );
                            phase_errors.push(format!("due job: {error:#}"));
                        }
                    }
                }
            }
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "recovery coordinator due claim phase failed"
                );
                phase_errors.push(format!("due claim: {error:#}"));
            }
        }

        if events.is_empty() && !phase_errors.is_empty() {
            bail!(
                "recovery coordinator phases failed: {}",
                phase_errors.join("; ")
            );
        }

        Ok(events)
    }

    /// Reconcile persisted native Turns that survived a process restart without
    /// an item timeout or provider-failure job.  The local AgentManager map is
    /// intentionally not consulted: it is process-local and cannot prove
    /// ownership after restart.  A recent durable activity frontier is the
    /// only reason to defer reconciliation.
    async fn reconcile_orphan_native_turns(&self, now_unix: i64, limit: u64) -> Result<()> {
        for turn in self.crud_store.list_in_progress_native_turns(limit).await? {
            if self
                .open_recovery_for_turn(turn.turn_id.as_str())
                .await?
                .is_some()
            {
                continue;
            }

            if self
                .crud_store
                .get_turn_liveness(turn.turn_id.as_str())
                .await?
                .is_some_and(|liveness| {
                    !liveness.last_activity_kind.starts_with("runtime/")
                        && now_unix.saturating_sub(liveness.last_activity_at_unix)
                            <= RECOVERY_PROGRESS_STALE_SECS
                })
            {
                continue;
            }

            let has_snapshot = self
                .crud_store
                .get_turn_runtime_snapshot(turn.turn_id.as_str())
                .await?
                .is_some();
            let policy = self
                .policy_registry
                .policy_for_item_type(TurnItemType::Reasoning);
            let action = if has_snapshot {
                RecoveryAction::RestartTurn
            } else {
                RecoveryAction::BlockResumable
            };
            let reason = if has_snapshot {
                "native Turn was orphaned by a restart; resuming from its durable runtime snapshot"
                    .to_owned()
            } else {
                "native Turn was orphaned by a restart without a durable runtime snapshot; preserving it as resumable blocked work"
                    .to_owned()
            };
            let orphan_turn_id = turn.turn_id.clone();
            let _ = self
                .enqueue_runtime_failure_job(
                    &RuntimeFailureCandidate {
                        turn_id: orphan_turn_id.clone(),
                        item_id: format!("orphan:{orphan_turn_id}"),
                        item_type: TurnItemType::Reasoning,
                        trigger: RecoveryTrigger::RuntimeFailure,
                        action,
                        reason,
                        base_backoff_secs: policy.base_backoff_secs,
                        max_attempts: if has_snapshot { policy.max_attempts } else { 0 },
                        max_wall_clock_secs: policy.max_wall_clock_secs,
                        no_progress_limit: policy.no_progress_limit,
                        metadata: ToolMetadata::empty(),
                    },
                    now_unix,
                )
                .await?;
        }
        Ok(())
    }

    pub async fn resume_blocked_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        recovery_job_id: Option<&str>,
        now_unix: i64,
    ) -> Result<Option<RecoveryJobRecord>> {
        let outcome = self
            .crud_store
            .resume_blocked_turn_recovery(thread_id, turn_id, recovery_job_id, now_unix)
            .await?;

        let job = match outcome {
            BlockedTurnRecoveryResumeOutcome::Resumed(job) => job,
            BlockedTurnRecoveryResumeOutcome::NotFound => return Ok(None),
            BlockedTurnRecoveryResumeOutcome::MissingRuntimeSnapshot { recovery_job_id } => {
                let _ = self
                    .crud_store
                    .mark_recovery_job_terminal(
                        recovery_job_id.as_str(),
                        RecoveryJobStatus::Blocked,
                        Some(
                            "blocked tool-item recovery cannot resume without a durable turn runtime snapshot"
                                .to_owned(),
                        ),
                        now_unix,
                    )
                    .await?;
                bail!(
                    "blocked recovery job `{}` is attached to a tool item and has no durable turn runtime snapshot",
                    recovery_job_id
                );
            }
        };

        Ok(Some(job))
    }

    pub async fn repair_due_terminal_recovery_jobs(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let jobs = self
            .crud_store
            .list_due_pending_recovery_jobs_by_action(RecoveryAction::MarkFailed, now_unix, limit)
            .await?;
        let mut events = Vec::new();

        for job in jobs {
            events.extend(
                self.terminalize_pending_mark_failed_job(job, now_unix)
                    .await?,
            );
        }

        Ok(events)
    }

    pub async fn terminalize_pending_mark_failed_job(
        &self,
        job: RecoveryJobRecord,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        if job.status != RecoveryJobStatus::Pending || job.action != RecoveryAction::MarkFailed {
            return Ok(Vec::new());
        }

        let attempt_number = attempt_number_for_job(&job);
        let message = terminal_policy_error_message(&job);
        if self
            .crud_store
            .mark_due_pending_recovery_job_terminal_if_turn_idle(
                job.id.as_str(),
                RecoveryAction::MarkFailed,
                RecoveryJobStatus::Failed,
                Some(message.clone()),
                now_unix,
            )
            .await?
        {
            self.cancel_other_open_jobs_after_terminal_recovery(
                job.turn_id.as_str(),
                job.id.as_str(),
                now_unix,
            )
            .await?;
            return Ok(vec![RecoveryCoordinatorEvent::RecoveryExhausted(
                RecoveryTerminalOutcome {
                    job_id: job.id,
                    turn_id: job.turn_id,
                    item_id: job.item_id,
                    item_type: job.item_type,
                    attempt_number,
                    status: RecoveryJobStatus::Failed,
                    error_message: message,
                },
            )]);
        }

        Ok(Vec::new())
    }

    async fn expire_active_recovery_jobs(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let jobs = self.crud_store.list_active_recovery_jobs(limit).await?;
        let mut events = Vec::new();

        for job in jobs {
            let policy = self.policy_for_recovery_job(&job).await?;
            let Some(max_wall_clock_secs) = i64::try_from(policy.max_wall_clock_secs).ok() else {
                continue;
            };
            let wall_clock_elapsed = now_unix.saturating_sub(job.scheduled_at_unix);
            let active_elapsed = job
                .active_attempt_started_at_unix
                .map(|started_at| now_unix.saturating_sub(started_at))
                .unwrap_or_else(|| now_unix.saturating_sub(job.updated_at_unix));
            let attempt_wall_clock_secs = i64::try_from(
                policy
                    .max_wall_clock_secs
                    .min(RECOVERY_ATTEMPT_MAX_WALL_CLOCK_SECS),
            )
            .unwrap_or(i64::MAX);
            let progress_frontier = self
                .crud_store
                .get_turn_liveness(job.turn_id.as_str())
                .await?;
            let progress_since_attempt = progress_frontier.as_ref().is_some_and(|liveness| {
                is_causal_recovery_progress(liveness.last_activity_kind.as_str())
                    && now_unix.saturating_sub(liveness.last_activity_at_unix)
                        <= RECOVERY_PROGRESS_STALE_SECS
                    && liveness.last_activity_at_unix
                        > job
                            .active_attempt_started_at_unix
                            .unwrap_or(job.updated_at_unix)
            });
            // Wall-clock expiry is only a no-progress episode guard.  A
            // provider stream that is durably emitting causal progress must
            // not be failed merely because recovery mode crossed the old
            // hidden 180-second ceiling.
            let job_budget_exceeded =
                wall_clock_elapsed > max_wall_clock_secs && !progress_since_attempt;
            let attempt_budget_exceeded =
                active_elapsed > attempt_wall_clock_secs && !progress_since_attempt;
            if !job_budget_exceeded && !attempt_budget_exceeded {
                continue;
            }

            let message = Some(if job_budget_exceeded {
                format!(
                    "recovery job exceeded {max_wall_clock_secs}s wall-clock budget after {wall_clock_elapsed}s (active attempt ran for {active_elapsed}s)"
                )
            } else {
                format!(
                    "recovery attempt exceeded {attempt_wall_clock_secs}s wall-clock budget after {active_elapsed}s"
                )
            });

            if let Some(active_attempt_id) = job.active_attempt_id.clone() {
                events.extend(
                    self.record_active_recovery_failure(
                        job,
                        active_attempt_id.as_str(),
                        message,
                        None,
                        now_unix,
                    )
                    .await?,
                );
            } else {
                let attempt_number = attempt_number_for_job(&job);
                if self
                    .crud_store
                    .mark_malformed_active_recovery_job_terminal(
                        job.id.as_str(),
                        RecoveryJobStatus::Failed,
                        message.clone(),
                        now_unix,
                    )
                    .await?
                {
                    self.cancel_other_open_jobs_after_terminal_recovery(
                        job.turn_id.as_str(),
                        job.id.as_str(),
                        now_unix,
                    )
                    .await?;
                    events.push(RecoveryCoordinatorEvent::RecoveryExhausted(
                        RecoveryTerminalOutcome {
                            job_id: job.id,
                            turn_id: job.turn_id,
                            item_id: job.item_id,
                            item_type: job.item_type,
                            attempt_number,
                            status: RecoveryJobStatus::Failed,
                            error_message: message.unwrap_or_else(|| {
                                "malformed active recovery job has no attempt id".to_owned()
                            }),
                        },
                    ));
                }
            }
        }

        Ok(events)
    }

    async fn run_single_job(
        &self,
        job: RecoveryJobRecord,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let mut events = Vec::new();

        let policy = self.policy_for_recovery_job(&job).await?;
        let Some(claim_token) = job.claim_token.clone() else {
            bail!("claimed recovery job `{}` has no claim token", job.id);
        };

        let run_index = job.run_count.saturating_add(1);

        let attempt_number = attempt_number_for_job(&job);

        if self
            .active_recovery_exists_for_turn(job.turn_id.as_str())
            .await?
        {
            let _ = self
                .crud_store
                .release_claimed_recovery_job(
                    job.id.as_str(),
                    claim_token.as_str(),
                    now_unix.saturating_add(ACTIVE_RECOVERY_RECHECK_SECS),
                    Some("another recovery is already active for this turn".to_owned()),
                    now_unix,
                )
                .await?;
            return Ok(events);
        }

        if policy.action == RecoveryAction::MarkFailed {
            let message = terminal_policy_error_message(&job);
            if self
                .crud_store
                .mark_claimed_recovery_job_terminal(
                    job.id.as_str(),
                    claim_token.as_str(),
                    RecoveryJobStatus::Failed,
                    Some(message.clone()),
                    now_unix,
                )
                .await?
            {
                self.cancel_other_open_jobs_after_terminal_recovery(
                    job.turn_id.as_str(),
                    job.id.as_str(),
                    now_unix,
                )
                .await?;
                events.push(RecoveryCoordinatorEvent::RecoveryExhausted(
                    RecoveryTerminalOutcome {
                        job_id: job.id,
                        turn_id: job.turn_id,
                        item_id: job.item_id,
                        item_type: job.item_type,
                        attempt_number,
                        status: RecoveryJobStatus::Failed,
                        error_message: message,
                    },
                ));
            }
            return Ok(events);
        }

        if policy.action == RecoveryAction::BlockResumable {
            let message = block_resumable_policy_message(&job);
            if self
                .crud_store
                .mark_claimed_recovery_job_terminal(
                    job.id.as_str(),
                    claim_token.as_str(),
                    RecoveryJobStatus::Blocked,
                    Some(message.clone()),
                    now_unix,
                )
                .await?
            {
                self.cancel_other_open_jobs_after_terminal_recovery(
                    job.turn_id.as_str(),
                    job.id.as_str(),
                    now_unix,
                )
                .await?;
                events.push(RecoveryCoordinatorEvent::RecoveryBlocked {
                    job_id: job.id,
                    turn_id: job.turn_id,
                    reason: message,
                });
            }
            return Ok(events);
        }

        let wall_clock_exceeded = i64::try_from(policy.max_wall_clock_secs)
            .ok()
            .is_some_and(|limit| now_unix.saturating_sub(job.scheduled_at_unix) > limit);

        let no_progress_exceeded =
            policy.no_progress_limit >= 0 && run_index > policy.no_progress_limit;

        let attempts_exhausted = policy.max_attempts >= 0 && run_index > policy.max_attempts;

        if wall_clock_exceeded || no_progress_exceeded || attempts_exhausted {
            let message = if wall_clock_exceeded {
                "recovery wall-clock budget exhausted".to_owned()
            } else if attempts_exhausted {
                "recovery attempts exhausted".to_owned()
            } else {
                "recovery no-progress guardrail exhausted".to_owned()
            };

            if self
                .crud_store
                .mark_claimed_recovery_job_terminal(
                    job.id.as_str(),
                    claim_token.as_str(),
                    RecoveryJobStatus::Exhausted,
                    Some(message.clone()),
                    now_unix,
                )
                .await?
            {
                self.cancel_other_open_jobs_after_terminal_recovery(
                    job.turn_id.as_str(),
                    job.id.as_str(),
                    now_unix,
                )
                .await?;
                events.push(RecoveryCoordinatorEvent::RecoveryExhausted(
                    RecoveryTerminalOutcome {
                        job_id: job.id,
                        turn_id: job.turn_id,
                        item_id: job.item_id,
                        item_type: job.item_type,
                        attempt_number,
                        status: RecoveryJobStatus::Exhausted,
                        error_message: message,
                    },
                ));
            }

            return Ok(events);
        }

        let Some((thread_id, workspace_id)) = self
            .crud_store
            .get_turn_location(job.turn_id.as_str())
            .await?
        else {
            let next_run_at_unix = backoff_deadline(
                now_unix,
                policy.base_backoff_secs,
                job.run_count,
                job.id.as_str(),
                job.retry_after_ms,
            );
            let reason = Some("turn location missing".to_owned());
            if self
                .crud_store
                .mark_claimed_recovery_job_retrying(
                    job.id.as_str(),
                    claim_token.as_str(),
                    next_run_at_unix,
                    reason.clone(),
                    now_unix,
                )
                .await?
            {
                events.push(RecoveryCoordinatorEvent::RetryScheduled {
                    job_id: job.id,
                    turn_id: job.turn_id,
                    item_id: job.item_id,
                    item_type: job.item_type,
                    attempt_number,
                    next_run_at_unix,
                    reason,
                });
            }
            return Ok(events);
        };

        if let Err(error) = self
            .revalidate_persisted_turn_execution_authorization(
                workspace_id.as_str(),
                thread_id.as_str(),
                job.turn_id.as_str(),
            )
            .await
        {
            warn!(
                turn_id = job.turn_id.as_str(),
                error = %format!("{error:#}"),
                "blocked recovery after initiating execution authority changed"
            );
            let reason =
                "automatic recovery blocked because initiating authority is no longer active"
                    .to_owned();
            if self
                .crud_store
                .mark_claimed_recovery_job_terminal(
                    job.id.as_str(),
                    claim_token.as_str(),
                    RecoveryJobStatus::Blocked,
                    Some(reason.clone()),
                    now_unix,
                )
                .await?
            {
                self.cancel_other_open_jobs_after_terminal_recovery(
                    job.turn_id.as_str(),
                    job.id.as_str(),
                    now_unix,
                )
                .await?;
                events.push(RecoveryCoordinatorEvent::RecoveryBlocked {
                    job_id: job.id,
                    turn_id: job.turn_id,
                    reason,
                });
            }
            return Ok(events);
        }

        if job.action == RecoveryAction::RehydrateTurnState {
            let Some(binding) = self
                .crud_store
                .get_cli_runtime_turn_binding(job.turn_id.as_str())
                .await?
            else {
                let reason =
                    "terminal reconciliation requires the durable CLI runtime binding".to_owned();
                if self
                    .crud_store
                    .mark_claimed_recovery_job_terminal(
                        job.id.as_str(),
                        claim_token.as_str(),
                        RecoveryJobStatus::Blocked,
                        Some(reason.clone()),
                        now_unix,
                    )
                    .await?
                {
                    events.push(RecoveryCoordinatorEvent::RecoveryBlocked {
                        job_id: job.id,
                        turn_id: job.turn_id,
                        reason,
                    });
                }
                return Ok(events);
            };
            let active_attempt_id = generate_id(RECOVERY_ATTEMPT_ID_LEN);
            match self
                .crud_store
                .mark_claimed_recovery_job_active(
                    job.id.as_str(),
                    claim_token.as_str(),
                    active_attempt_id.as_str(),
                    now_unix,
                )
                .await?
            {
                ClaimedRecoveryActivation::Activated => {}
                ClaimedRecoveryActivation::BlockedByActiveRecovery => {
                    let _ = self
                        .crud_store
                        .release_claimed_recovery_job(
                            job.id.as_str(),
                            claim_token.as_str(),
                            now_unix.saturating_add(ACTIVE_RECOVERY_RECHECK_SECS),
                            Some("another recovery is already active for this turn".to_owned()),
                            now_unix,
                        )
                        .await?;
                    return Ok(events);
                }
                ClaimedRecoveryActivation::ClaimNotFound => return Ok(events),
            }
            events.push(
                RecoveryCoordinatorEvent::CliRuntimeTerminalReconciliationRequested(Box::new(
                    CliRuntimeTerminalReconciliationRequest {
                        job_id: job.id,
                        recovery_attempt_id: active_attempt_id,
                        turn_id: job.turn_id,
                        item_id: job.item_id,
                        item_type: job.item_type,
                        attempt_number,
                        binding,
                    },
                )),
            );
            return Ok(events);
        }

        let execution_plan = self.build_attempt_plan(&job, attempt_number).await?;

        if let Some(message) = execution_plan.terminal_reason.clone() {
            if self
                .crud_store
                .mark_claimed_recovery_job_terminal(
                    job.id.as_str(),
                    claim_token.as_str(),
                    RecoveryJobStatus::Failed,
                    Some(message.clone()),
                    now_unix,
                )
                .await?
            {
                self.cancel_other_open_jobs_after_terminal_recovery(
                    job.turn_id.as_str(),
                    job.id.as_str(),
                    now_unix,
                )
                .await?;
                events.push(RecoveryCoordinatorEvent::RecoveryExhausted(
                    RecoveryTerminalOutcome {
                        job_id: job.id,
                        turn_id: job.turn_id,
                        item_id: job.item_id,
                        item_type: job.item_type,
                        attempt_number,
                        status: RecoveryJobStatus::Failed,
                        error_message: message,
                    },
                ));
            }
            return Ok(events);
        }

        let cli_runtime_binding = if job.action == RecoveryAction::RestartTurn {
            self.crud_store
                .get_cli_runtime_turn_binding(job.turn_id.as_str())
                .await?
        } else {
            None
        };
        let active_attempt_id = generate_id(RECOVERY_ATTEMPT_ID_LEN);
        match self
            .crud_store
            .mark_claimed_recovery_job_active(
                job.id.as_str(),
                claim_token.as_str(),
                active_attempt_id.as_str(),
                now_unix,
            )
            .await?
        {
            ClaimedRecoveryActivation::Activated => {}
            ClaimedRecoveryActivation::BlockedByActiveRecovery => {
                let _ = self
                    .crud_store
                    .release_claimed_recovery_job(
                        job.id.as_str(),
                        claim_token.as_str(),
                        now_unix.saturating_add(ACTIVE_RECOVERY_RECHECK_SECS),
                        Some("another recovery is already active for this turn".to_owned()),
                        now_unix,
                    )
                    .await?;
                return Ok(events);
            }
            ClaimedRecoveryActivation::ClaimNotFound => return Ok(events),
        }

        if let Some(binding) = cli_runtime_binding {
            let execution_window_index = match self
                .prepare_recovery_execution_window(
                    job.turn_id.as_str(),
                    now_unix,
                    "cli_runtime_recovery",
                    "native CLI turn was replaced by a recovery attempt",
                )
                .await
            {
                Ok(ExecutionWindowContinuationAdmission::Open { window_index }) => window_index,
                Ok(ExecutionWindowContinuationAdmission::Block { reason, .. }) => {
                    return self
                        .block_active_recovery(job, active_attempt_id, reason, now_unix)
                        .await;
                }
                Err(error) => {
                    return self
                        .record_active_recovery_failure(
                            job,
                            active_attempt_id.as_str(),
                            Some(format!(
                                "failed to prepare CLI runtime recovery execution window: {error:#}"
                            )),
                            None,
                            now_unix,
                        )
                        .await;
                }
            };
            events.push(RecoveryCoordinatorEvent::CliRuntimeRetryAttemptRequested(
                Box::new(CliRuntimeRecoveryAttemptRequest {
                    job_id: job.id,
                    recovery_attempt_id: active_attempt_id,
                    turn_id: job.turn_id,
                    item_id: job.item_id,
                    item_type: job.item_type,
                    attempt_number,
                    execution_window_index,
                    previous_failure_reason: job
                        .last_error
                        .or(job.reason)
                        .unwrap_or_else(|| "native CLI turn was interrupted".to_owned()),
                    binding,
                }),
            ));
            return Ok(events);
        }

        let retained_provider_history = match self
            .retained_provider_history_for_turn(job.turn_id.as_str())
            .await
        {
            Ok(history) => history,
            Err(error) => {
                return self
                    .block_active_recovery(
                        job,
                        active_attempt_id,
                        format!(
                            "automatic recovery cannot safely replay the provider history: {error:#}"
                        ),
                        now_unix,
                    )
                    .await;
            }
        };
        let execution_checkpoint_context = self
            .prepare_execution_checkpoint_for_recovery(
                workspace_id.as_str(),
                thread_id.as_str(),
                &job,
                now_unix,
            )
            .await?;
        if let Some(context) = execution_checkpoint_context.as_ref()
            && let ExecutionWindowContinuationAdmission::Block { reason, .. } = self
                .execution_window_continuation_admission_for_turn(
                    job.turn_id.as_str(),
                    Some(context.window_index),
                )
                .await?
        {
            return self
                .block_active_recovery(job, active_attempt_id, reason, now_unix)
                .await;
        }
        let continue_generation =
            execution_plan.continue_generation || execution_checkpoint_context.is_some();

        let mut request = RecoveryAttemptRequest {
            recovery_job_id: job.id.clone(),
            recovery_attempt_id: active_attempt_id.clone(),
            turn_id: job.turn_id.clone(),
            item_id: job.item_id.clone(),
            item_type: job.item_type,
            force_non_stream: execution_plan.force_non_stream,
            disable_tool_calling: execution_plan.disable_tool_calling,
            disable_image_input: execution_plan.disable_image_input,
            refresh_provider_auth: execution_plan.refresh_provider_auth,
            compact_history: execution_plan.compact_history,
            continue_generation,
            model_override: execution_plan.model_override,
            retained_provider_history,
            execution_checkpoint_context,
        };

        if self.agent_manager.has_thread(thread_id.as_str()).await
            && let Err(error) = self.ensure_recovery_listener(thread_id.as_str()).await
        {
            return self
                .handle_recovery_start_error(
                    job,
                    active_attempt_id,
                    error,
                    policy,
                    attempt_number,
                    now_unix,
                )
                .await;
        }

        match self
            .agent_manager
            .start_recovery_attempt(thread_id.as_str(), request.clone())
            .await
        {
            Ok(()) => {
                events.push(RecoveryCoordinatorEvent::RetryAttemptStarted {
                    job_id: job.id.clone(),
                    turn_id: job.turn_id.clone(),
                    item_id: job.item_id.clone(),
                    item_type: job.item_type,
                    attempt_number,
                });
            }
            Err(error) => {
                if matches!(
                    &error,
                    AgentControlError::ThreadNotFound | AgentControlError::NoActiveTurn
                ) {
                    match self
                        .restored_recovery_turn_request(
                            thread_id.as_str(),
                            job.turn_id.as_str(),
                            now_unix,
                        )
                        .await?
                    {
                        RestoredRecoveryTurnRequestLookup::Available(restored_turn_request) => {
                            request.execution_checkpoint_context = self
                                .execution_checkpoint_context_for_turn(
                                    thread_id.as_str(),
                                    job.turn_id.as_str(),
                                )
                                .await?;
                            request.continue_generation = request.continue_generation
                                || request.execution_checkpoint_context.is_some();
                            if let Err(error) = self
                                .agent_manager
                                .ensure_thread(thread_id.as_str(), workspace_id.as_str())
                                .await
                            {
                                return self
                                    .handle_recovery_start_error(
                                        job,
                                        active_attempt_id,
                                        AgentControlError::Internal(error.to_string()),
                                        policy,
                                        attempt_number,
                                        now_unix,
                                    )
                                    .await;
                            }
                            if let Err(error) =
                                self.ensure_recovery_listener(thread_id.as_str()).await
                            {
                                return self
                                    .handle_recovery_start_error(
                                        job,
                                        active_attempt_id,
                                        error,
                                        policy,
                                        attempt_number,
                                        now_unix,
                                    )
                                    .await;
                            }
                            match self
                                .agent_manager
                                .start_restored_recovery_turn(
                                    thread_id.as_str(),
                                    workspace_id.as_str(),
                                    restored_turn_request,
                                    request.clone(),
                                )
                                .await
                            {
                                Ok(()) => {
                                    events.push(RecoveryCoordinatorEvent::RetryAttemptStarted {
                                        job_id: job.id.clone(),
                                        turn_id: job.turn_id.clone(),
                                        item_id: job.item_id.clone(),
                                        item_type: job.item_type,
                                        attempt_number,
                                    });
                                    return Ok(events);
                                }
                                Err(restored_error) => {
                                    return self
                                        .handle_recovery_start_error(
                                            job,
                                            active_attempt_id,
                                            restored_error,
                                            policy,
                                            attempt_number,
                                            now_unix,
                                        )
                                        .await;
                                }
                            }
                        }
                        RestoredRecoveryTurnRequestLookup::Unavailable(unavailable) => {
                            if let Some(reason) = unavailable.lost_loop_block_reason() {
                                return self
                                    .block_active_recovery(job, active_attempt_id, reason, now_unix)
                                    .await;
                            }
                        }
                    }
                }

                return self
                    .handle_recovery_start_error(
                        job,
                        active_attempt_id,
                        error,
                        policy,
                        attempt_number,
                        now_unix,
                    )
                    .await;
            }
        }

        Ok(events)
    }

    async fn block_active_recovery(
        &self,
        job: RecoveryJobRecord,
        active_attempt_id: String,
        reason: String,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let mut events = Vec::new();
        if self
            .crud_store
            .mark_recovery_job_terminal_after_attempt(
                job.id.as_str(),
                active_attempt_id.as_str(),
                RecoveryJobStatus::Blocked,
                Some(reason.clone()),
                now_unix,
            )
            .await?
        {
            self.cancel_other_open_jobs_after_terminal_recovery(
                job.turn_id.as_str(),
                job.id.as_str(),
                now_unix,
            )
            .await?;
            events.push(RecoveryCoordinatorEvent::RecoveryBlocked {
                job_id: job.id,
                turn_id: job.turn_id,
                reason,
            });
        }

        Ok(events)
    }

    async fn handle_recovery_start_error(
        &self,
        job: RecoveryJobRecord,
        active_attempt_id: String,
        error: AgentControlError,
        policy: RecoveryPolicy,
        attempt_number: u32,
        now_unix: i64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        if let AgentControlError::ExecutionWindowContinuationBlocked { reason } = &error {
            return self
                .block_active_recovery(job, active_attempt_id, reason.clone(), now_unix)
                .await;
        }

        let mut events = Vec::new();
        let terminal = matches!(
            &error,
            AgentControlError::TurnMismatch | AgentControlError::ThreadNotFound
        );
        if terminal {
            let message = error.to_string();
            if self
                .crud_store
                .mark_recovery_job_terminal_after_attempt(
                    job.id.as_str(),
                    active_attempt_id.as_str(),
                    RecoveryJobStatus::Failed,
                    Some(message.clone()),
                    now_unix,
                )
                .await?
            {
                self.cancel_other_open_jobs_after_terminal_recovery(
                    job.turn_id.as_str(),
                    job.id.as_str(),
                    now_unix,
                )
                .await?;
                events.push(RecoveryCoordinatorEvent::RecoveryExhausted(
                    RecoveryTerminalOutcome {
                        job_id: job.id,
                        turn_id: job.turn_id,
                        item_id: job.item_id,
                        item_type: job.item_type,
                        attempt_number,
                        status: RecoveryJobStatus::Failed,
                        error_message: message,
                    },
                ));
            }
        } else {
            let next_run_at_unix = backoff_deadline(
                now_unix,
                policy.base_backoff_secs,
                job.run_count,
                job.id.as_str(),
                job.retry_after_ms,
            );
            let reason = Some(error.to_string());
            if self
                .crud_store
                .mark_recovery_job_retrying(
                    job.id.as_str(),
                    active_attempt_id.as_str(),
                    next_run_at_unix,
                    reason.clone(),
                    now_unix,
                )
                .await?
            {
                events.push(RecoveryCoordinatorEvent::RetryScheduled {
                    job_id: job.id,
                    turn_id: job.turn_id,
                    item_id: job.item_id,
                    item_type: job.item_type,
                    attempt_number,
                    next_run_at_unix,
                    reason,
                });
            }
        }

        Ok(events)
    }

    async fn execution_checkpoint_context_for_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<Option<ExecutionCheckpointContext>> {
        let Some((_workspace_id, turn)) = self.crud_store.get_turn(thread_id, turn_id).await?
        else {
            return Ok(None);
        };
        if turn.status != TurnStatus::InProgress {
            return Ok(None);
        }

        let Some(window) = self
            .crud_store
            .latest_turn_execution_window(turn_id)
            .await?
        else {
            return Ok(None);
        };
        if !matches!(
            window.status,
            ExecutionWindowStatus::Exhausted
                | ExecutionWindowStatus::Checkpointed
                | ExecutionWindowStatus::Continued
        ) {
            return Ok(None);
        }

        let Some(checkpoint) = self
            .crud_store
            .list_turn_execution_checkpoints_for_window(window.id.as_str())
            .await?
            .into_iter()
            .last()
        else {
            return Ok(None);
        };

        let Ok(payload) =
            serde_json::from_value::<ExecutionCheckpointPayload>(checkpoint.payload_json.clone())
        else {
            return Ok(None);
        };
        // Continuation notifications carry the runtime window identity.  The
        // checkpoint's window_id is a database foreign key and must not be
        // leaked into the provider/runtime event contract.
        let runtime_window_id = window
            .metadata_json
            .get("runtimeWindowId")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                payload
                    .window
                    .window_id
                    .as_ref()
                    .filter(|id| id.as_str() != window.id.as_str())
                    .cloned()
            })
            .unwrap_or_else(|| format!("{turn_id}:window:{}", window.window_index));

        Ok(Some(ExecutionCheckpointContext {
            window_id: runtime_window_id,
            window_index: window.window_index,
            checkpoint_id: checkpoint.id,
            checkpoint_kind: checkpoint_kind_label(checkpoint.checkpoint_kind),
            payload,
            usage: crate::turn_runtime_snapshot::execution_window_usage_snapshot(
                self.crud_store.as_ref(),
                turn_id,
            )
            .await?,
        }))
    }

    async fn prepare_execution_checkpoint_for_recovery(
        &self,
        workspace_id: &str,
        thread_id: &str,
        job: &RecoveryJobRecord,
        now_unix: i64,
    ) -> Result<Option<ExecutionCheckpointContext>> {
        let existing = self
            .execution_checkpoint_context_for_turn(thread_id, job.turn_id.as_str())
            .await?;
        if existing.is_some()
            || job.item_type.is_tool_item()
            || !matches!(
                job.trigger,
                RecoveryTrigger::Timeout
                    | RecoveryTrigger::ProviderError
                    | RecoveryTrigger::ExecutionWindowContinuation
                    | RecoveryTrigger::RuntimeFailure
            )
        {
            return Ok(existing);
        }

        let Some(snapshot) = self
            .crud_store
            .get_turn_runtime_snapshot(job.turn_id.as_str())
            .await?
        else {
            return Ok(None);
        };
        if snapshot.workspace_id != workspace_id || snapshot.thread_id != thread_id {
            return Ok(None);
        }
        let input = serde_json::from_str::<Vec<UserInput>>(snapshot.input_json.as_str())?;
        let reason = if job.trigger == RecoveryTrigger::ProviderError {
            ExecutionWindowExhaustionReason::ProviderFailureContinuation
        } else {
            ExecutionWindowExhaustionReason::RuntimeShutdownContinuation
        };
        let interrupted_by =
            if reason == ExecutionWindowExhaustionReason::ProviderFailureContinuation {
                "provider_failure_recovery"
            } else {
                "runtime_failure_recovery"
            };
        let terminal_reason = job
            .last_error
            .as_deref()
            .or(job.reason.as_deref())
            .unwrap_or("recoverable turn execution failure");

        self.checkpoint_running_execution_window(
            workspace_id,
            thread_id,
            job.turn_id.as_str(),
            input.as_slice(),
            snapshot.model.as_str(),
            snapshot.provider_name.as_str(),
            now_unix,
            reason,
            TurnExecutionCheckpointKind::WindowExhausted,
            interrupted_by,
            terminal_reason,
        )
        .await?;

        self.execution_checkpoint_context_for_turn(thread_id, job.turn_id.as_str())
            .await
    }

    async fn restored_recovery_turn_request(
        &self,
        thread_id: &str,
        turn_id: &str,
        now_unix: i64,
    ) -> Result<RestoredRecoveryTurnRequestLookup> {
        let Some((workspace_id, turn)) = self.crud_store.get_turn(thread_id, turn_id).await? else {
            return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                RestoredRecoveryTurnUnavailable::TurnNotFound,
            ));
        };
        if turn.status != TurnStatus::InProgress {
            return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                RestoredRecoveryTurnUnavailable::TurnNotInProgress,
            ));
        }

        let Some(snapshot) = self.crud_store.get_turn_runtime_snapshot(turn_id).await? else {
            return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                RestoredRecoveryTurnUnavailable::MissingRuntimeSnapshot,
            ));
        };
        if snapshot.thread_id != thread_id || snapshot.workspace_id != workspace_id {
            warn!(
                thread_id,
                turn_id,
                workspace_id,
                snapshot_thread_id = snapshot.thread_id,
                snapshot_workspace_id = snapshot.workspace_id,
                "turn runtime snapshot does not match recovery target"
            );
            return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                RestoredRecoveryTurnUnavailable::SnapshotMismatch,
            ));
        }

        if let Err(error) = self
            .revalidate_persisted_turn_execution_authorization(
                workspace_id.as_str(),
                thread_id,
                turn_id,
            )
            .await
        {
            return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                RestoredRecoveryTurnUnavailable::SnapshotInvalid {
                    error: format!(
                        "failed to revalidate current skill projection before recovery: {error:#}"
                    ),
                },
            ));
        }

        let agent_skill_overlay =
            match crate::turn_runtime_snapshot::restore_agent_skill_overlay_from_snapshot(
                self.crud_store.as_ref(),
                workspace_id.as_str(),
                &snapshot,
            )
            .await
            {
                Ok(overlay) => overlay,
                Err(error) => {
                    return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                        RestoredRecoveryTurnUnavailable::SnapshotInvalid {
                            error: format!(
                                "failed to restore exact pinned Agent skill versions: {error:#}"
                            ),
                        },
                    ));
                }
            };

        let mut request =
            match crate::turn_runtime_snapshot::restored_recovery_turn_request_from_snapshot(
                &snapshot,
                turn.permission_profile,
                match self
                    .crud_store
                    .get_turn_execution_security_snapshot(turn_id)
                    .await
                {
                    Ok(Some(security_snapshot)) => security_snapshot.snapshot,
                    Ok(None) => {
                        return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                            RestoredRecoveryTurnUnavailable::MissingExecutionSecuritySnapshot,
                        ));
                    }
                    Err(error) => {
                        return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                            RestoredRecoveryTurnUnavailable::SnapshotInvalid {
                                error: format!("{error:#}"),
                            },
                        ));
                    }
                },
                agent_skill_overlay,
            ) {
                Ok(request) => request,
                Err(error) => {
                    return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                        RestoredRecoveryTurnUnavailable::SnapshotInvalid {
                            error: format!("{error:#}"),
                        },
                    ));
                }
            };
        let skills_context =
            match crate::message::skills::workspace::skills_runtime_context_from_config(
                &self.tool_loop_config,
                workspace_id.as_str(),
            ) {
                Ok(context) => context,
                Err(error) => {
                    return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                        RestoredRecoveryTurnUnavailable::SnapshotInvalid {
                            error: format!("failed to resolve recovery skills context: {error:#}"),
                        },
                    ));
                }
            };
        request.skill_catalog =
            match crate::message::skills::workspace::load_skills_catalog_from_store(
                self.crud_store.as_ref(),
                workspace_id.as_str(),
                &skills_context,
            )
            .await
            {
                Ok(catalog) => catalog,
                Err(error) => {
                    return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                        RestoredRecoveryTurnUnavailable::SnapshotInvalid {
                            error: format!("failed to load recovery skill catalog: {error:#}"),
                        },
                    ));
                }
            };
        self.checkpoint_running_execution_window(
            workspace_id.as_str(),
            thread_id,
            request.turn_id.as_str(),
            request.input.as_slice(),
            request.model.as_str(),
            request.provider_name.as_str(),
            now_unix,
            ExecutionWindowExhaustionReason::RuntimeShutdownContinuation,
            TurnExecutionCheckpointKind::StartupRecovery,
            "startup_recovery",
            "agent loop was restored after process loss",
        )
        .await?;
        match self
            .execution_window_continuation_admission_for_turn(
                request.turn_id.as_str(),
                Some(request.execution_window_index),
            )
            .await?
        {
            ExecutionWindowContinuationAdmission::Open { window_index } => {
                request.execution_window_index = window_index;
            }
            ExecutionWindowContinuationAdmission::Block { reason, .. } => {
                return Ok(RestoredRecoveryTurnRequestLookup::Unavailable(
                    RestoredRecoveryTurnUnavailable::ExecutionWindowContinuationBlocked { reason },
                ));
            }
        }
        Ok(RestoredRecoveryTurnRequestLookup::Available(request))
    }

    async fn revalidate_persisted_turn_execution_authorization(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) -> Result<()> {
        let context = crate::authorization::ExecutionAuthorizationContext::load_for_turn(
            self.crud_store.as_ref(),
            turn_id,
        )
        .await
        .context("failed to load persisted execution authorization")?;
        let revalidated = self
            .execution_leases
            .revalidate_for_turn(
                self.crud_store.as_ref(),
                &context,
                workspace_id,
                thread_id,
                turn_id,
                crate::authorization::ResourceAction::AgentTurnStart,
                self.authorization_invalidation_hub
                    .current_revision()
                    .await
                    .context("recovery policy generation is unavailable")?,
            )
            .await
            .context("current execution authorization no longer permits recovery")?;

        let bindings = self
            .crud_store
            .find_turn_skill_bindings(turn_id)
            .await
            .context("failed to load persisted recovery skill bindings")?
            .into_iter()
            .map(|binding| pioneer_protocol::TurnSkillBinding {
                skill_id: binding.skill_id,
                skill_owner: binding.skill_owner,
                skill_slug: binding.skill_slug,
                skill_version: binding.skill_version,
                fingerprint: binding.fingerprint,
                source_kind: binding.source_kind,
                resolved_reason: binding.resolved_reason,
            })
            .collect::<Vec<_>>();
        if context.skill_projection().is_some() || !bindings.is_empty() {
            context
                .verify_skill_projection(workspace_id, bindings.as_slice())
                .context("persisted recovery skill projection is stale or unbound")?;
        }

        if revalidated.resource_boundary()
            == crate::authorization::ExecutionResourceBoundary::RootThreadCapsule
        {
            let action_gate = crate::authorization::AuthorizationService::new().authorize_action(
                revalidated.principal().kind,
                revalidated.principal().role_key.as_ref(),
                crate::authorization::ResourceAction::SkillUse,
            );
            let resolver =
                crate::authorization::AuthorizationResolver::new(self.crud_store.as_ref().clone());
            let database = self.crud_store.database_connection();
            let active_learned = self
                .crud_store
                .list_active_agent_skill_versions(workspace_id)
                .await?
                .into_iter()
                .map(|version| (version.skill_id.clone(), version))
                .collect::<HashMap<_, _>>();
            for binding in &bindings {
                let authorization = resolver
                    .authorize_persisted_capability(
                        revalidated.principal(),
                        &action_gate,
                        crate::authorization::ResourceAction::SkillUse,
                        workspace_id,
                        crate::authorization::CapabilityKind::Skill,
                        binding.skill_id.as_str(),
                    )
                    .await?;
                if !matches!(
                    authorization,
                    crate::authorization::ProofResolution::Authorized(_)
                ) {
                    bail!("current workspace policy no longer permits a projected recovery skill");
                }
                if binding.source_kind == "agent" {
                    let active = active_learned
                        .get(&binding.skill_id)
                        .context("projected learned recovery skill is no longer active")?;
                    if active.version.fingerprint != binding.fingerprint
                        || pioneer_crud::derive_member_learned_version_eligibility(
                            &database,
                            workspace_id,
                            active.version.id.as_str(),
                        )
                        .await?
                            != pioneer_crud::MemberLearnedVersionEligibility::Eligible
                    {
                        bail!("projected learned recovery skill is no longer eligible");
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn checkpoint_running_execution_window(
        &self,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        input: &[UserInput],
        model: &str,
        provider_name: &str,
        now_unix: i64,
        reason: ExecutionWindowExhaustionReason,
        checkpoint_kind: TurnExecutionCheckpointKind,
        interrupted_by: &str,
        terminal_reason: &str,
    ) -> Result<()> {
        let completed_at = chrono::DateTime::<chrono::Utc>::from_timestamp(now_unix, 0)
            .map(|timestamp| timestamp.fixed_offset())
            .unwrap_or_else(|| chrono::Utc::now().fixed_offset());
        let window = match self
            .crud_store
            .latest_turn_execution_window(turn_id)
            .await?
        {
            Some(window) => window,
            None => {
                self.crud_store
                    .create_turn_execution_window(
                        NewTurnExecutionWindowRecord {
                            workspace_id: workspace_id.to_owned(),
                            thread_id: thread_id.to_owned(),
                            turn_id: turn_id.to_owned(),
                            window_index: 1,
                            status: ExecutionWindowStatus::Running,
                            exhaustion_reason: None,
                            agent_round_count: 0,
                            tool_call_count: 0,
                            provider_token_count: 0,
                            metadata_json: serde_json::json!({
                                "runtimeWindowId": format!("{turn_id}:window:1"),
                                "recoveredFromMissingWindow": true,
                            }),
                            started_at: completed_at,
                        },
                        completed_at,
                        completed_at,
                    )
                    .await?
            }
        };

        if window.status != ExecutionWindowStatus::Running {
            return Ok(());
        }

        let window_items = self
            .crud_store
            .list_turn_execution_window_items(turn_id, window.started_at.clone())
            .await?;
        let terminal_counts = self
            .crud_store
            .count_turn_execution_window_terminal_items_since(turn_id, window.started_at.clone())
            .await?;
        let agent_round_count = window
            .agent_round_count
            .max(terminal_counts.agent_round_count);
        let tool_summary = build_execution_checkpoint_tool_summary(
            window_items.as_slice(),
            EXECUTION_CHECKPOINT_DEFAULT_TOOL_DETAIL_LIMIT,
        );
        let tool_call_count = window.tool_call_count.max(
            terminal_counts
                .tool_call_count
                .max(tool_summary.requested_count),
        );
        let provider_token_count =
            (window.provider_token_count > 0).then_some(window.provider_token_count);
        // The provider-facing checkpoint payload carries the logical runtime
        // window identity. `window.id` is only the database row key used by
        // the checkpoint foreign key and must never cross the recovery
        // protocol boundary. Legacy rows without metadata get the same
        // deterministic identity used when a missing window is materialized.
        let runtime_window_id = window
            .metadata_json
            .get("runtimeWindowId")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{turn_id}:window:{}", window.window_index));
        let payload = build_execution_checkpoint_payload(
            workspace_id,
            thread_id,
            turn_id,
            build_execution_checkpoint_original_request_summary(input),
            ExecutionCheckpointWindowSummary {
                window_id: Some(runtime_window_id),
                window_index: window.window_index,
                started_at_unix_ms: Some(window.started_at.timestamp_millis()),
                completed_at_unix_ms: Some(completed_at.timestamp_millis()),
                agent_round_count,
                tool_call_count,
                provider_token_count,
                exhaustion_reason: Some(reason),
            },
            build_execution_checkpoint_provider_budget_summary(
                ExecutionCheckpointProviderBudgetInput {
                    model: Some(model.to_owned()),
                    model_provider: Some(provider_name.to_owned()),
                    agent_round_count,
                    tool_call_count,
                    provider_token_count,
                    exhaustion_reason: Some(reason),
                    exhausted_limit: None,
                    exhausted_observed: None,
                },
            ),
            tool_summary,
            Vec::new(),
        );
        let mut metadata_json = window.metadata_json.clone();
        match metadata_json.as_object_mut() {
            Some(metadata) => {
                metadata.insert(
                    "interruptedBy".to_owned(),
                    serde_json::Value::String(interrupted_by.to_owned()),
                );
                metadata.insert(
                    "terminalReason".to_owned(),
                    serde_json::Value::String(terminal_reason.to_owned()),
                );
            }
            None => {
                metadata_json = serde_json::json!({
                    "interruptedBy": interrupted_by,
                    "terminalReason": terminal_reason,
                });
            }
        }

        let checkpoint_exists = self
            .crud_store
            .list_turn_execution_checkpoints_for_window(window.id.as_str())
            .await?
            .iter()
            .any(|checkpoint| checkpoint.checkpoint_kind == checkpoint_kind);
        if !checkpoint_exists {
            self.crud_store
                .save_turn_execution_checkpoint(NewTurnExecutionCheckpointRecord {
                    id: None,
                    window_id: window.id.clone(),
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    checkpoint_kind,
                    payload_json: serde_json::to_value(payload)?,
                    created_at: completed_at.clone(),
                })
                .await?;
        }
        self.crud_store
            .mark_turn_execution_window_exhausted(
                window.id.as_str(),
                reason,
                pioneer_crud::TurnExecutionWindowStatsRecord {
                    agent_round_count,
                    tool_call_count,
                    provider_token_count: window.provider_token_count,
                    metadata_json,
                    completed_at: completed_at.clone(),
                    updated_at: completed_at.clone(),
                },
            )
            .await?;
        self.crud_store
            .mark_turn_execution_window_checkpointed(window.id.as_str(), completed_at)
            .await?;

        Ok(())
    }

    async fn execution_window_admission_for_turn(
        &self,
        turn_id: &str,
        observed_window_index: Option<u32>,
    ) -> Result<ExecutionWindowAdmissionDecision> {
        let stored_window_index = self
            .crud_store
            .latest_turn_execution_window(turn_id)
            .await?
            .map(|window| window.window_index);
        let legacy_cli_window_index =
            if stored_window_index.is_none() && observed_window_index.is_none() {
                let has_cli_execution = self
                    .crud_store
                    .latest_cli_runtime_turn_attempt(turn_id)
                    .await?
                    .is_some()
                    || self
                        .crud_store
                        .get_cli_runtime_turn_binding(turn_id)
                        .await?
                        .is_some();
                has_cli_execution.then_some(1)
            } else {
                None
            };
        let latest_window_index = [
            observed_window_index,
            stored_window_index,
            legacy_cli_window_index,
        ]
        .into_iter()
        .flatten()
        .filter(|index| *index > 0)
        .max();

        Ok(decide_execution_window_admission(
            latest_window_index,
            self.tool_loop_config
                .execution_windows
                .total
                .max_windows_per_turn,
        ))
    }

    async fn execution_window_continuation_admission_for_turn(
        &self,
        turn_id: &str,
        observed_window_index: Option<u32>,
    ) -> Result<ExecutionWindowContinuationAdmission> {
        match self
            .execution_window_admission_for_turn(turn_id, observed_window_index)
            .await?
        {
            ExecutionWindowAdmissionDecision::Open { window_index } => {
                let usage = crate::turn_runtime_snapshot::execution_window_usage_snapshot(
                    self.crud_store.as_ref(),
                    turn_id,
                )
                .await?;
                let total_budget = &self.tool_loop_config.execution_windows.total;
                if let Some(limit) = total_budget.max_tool_calls_per_turn
                    && usage.total_tool_calls >= u64::from(limit)
                {
                    return Ok(ExecutionWindowContinuationAdmission::Block {
                        total_windows: usage.total_windows,
                        reason: format!(
                            "max_total_tool_calls_per_turn reached: limit={limit}, observed={}",
                            usage.total_tool_calls
                        ),
                    });
                }
                if let Some(limit) = total_budget.max_wall_clock_ms_per_turn
                    && usage.total_wall_clock_ms >= limit
                {
                    return Ok(ExecutionWindowContinuationAdmission::Block {
                        total_windows: usage.total_windows,
                        reason: format!(
                            "max_total_wall_clock_ms_per_turn reached: limit={limit}, observed={}",
                            usage.total_wall_clock_ms
                        ),
                    });
                }
                if let Some(limit) = total_budget.max_provider_tokens_per_turn
                    && !usage.provider_token_usage_unknown
                    && usage.total_provider_tokens >= limit
                {
                    return Ok(ExecutionWindowContinuationAdmission::Block {
                        total_windows: usage.total_windows,
                        reason: format!(
                            "max_total_provider_tokens_per_turn reached: limit={limit}, observed={}",
                            usage.total_provider_tokens
                        ),
                    });
                }
                let limit = self
                    .tool_loop_config
                    .execution_windows
                    .total
                    .max_consecutive_no_progress_windows
                    .max(1);
                if usage.consecutive_no_progress_windows >= limit {
                    return Ok(ExecutionWindowContinuationAdmission::Block {
                        total_windows: usage.total_windows,
                        reason: execution_window_no_progress_reason(
                            limit,
                            usage.consecutive_no_progress_windows,
                        ),
                    });
                }
                Ok(ExecutionWindowContinuationAdmission::Open { window_index })
            }
            ExecutionWindowAdmissionDecision::Block {
                total_windows,
                max_windows_per_turn,
            } => Ok(ExecutionWindowContinuationAdmission::Block {
                total_windows,
                reason: execution_window_limit_reason(total_windows, max_windows_per_turn),
            }),
        }
    }

    async fn prepare_recovery_execution_window(
        &self,
        turn_id: &str,
        now_unix: i64,
        interrupted_by: &str,
        terminal_reason: &str,
    ) -> Result<ExecutionWindowContinuationAdmission> {
        let mut window = self
            .crud_store
            .latest_turn_execution_window(turn_id)
            .await?;
        if window.is_none() {
            let has_cli_execution = self
                .crud_store
                .latest_cli_runtime_turn_attempt(turn_id)
                .await?
                .is_some()
                || self
                    .crud_store
                    .get_cli_runtime_turn_binding(turn_id)
                    .await?
                    .is_some();
            if has_cli_execution
                && let Some((thread_id, workspace_id)) =
                    self.crud_store.get_turn_location(turn_id).await?
            {
                let completed_at = chrono::DateTime::<chrono::Utc>::from_timestamp(now_unix, 0)
                    .map(|timestamp| timestamp.fixed_offset())
                    .unwrap_or_else(|| chrono::Utc::now().fixed_offset());
                window = Some(
                    self.crud_store
                        .create_turn_execution_window(
                            NewTurnExecutionWindowRecord {
                                workspace_id,
                                thread_id,
                                turn_id: turn_id.to_owned(),
                                window_index: 1,
                                status: ExecutionWindowStatus::Running,
                                exhaustion_reason: None,
                                agent_round_count: 0,
                                tool_call_count: 0,
                                provider_token_count: 0,
                                metadata_json: serde_json::json!({
                                    "interruptedBy": interrupted_by,
                                    "terminalReason": terminal_reason,
                                    "recoveredFromMissingWindow": true,
                                }),
                                started_at: completed_at,
                            },
                            completed_at,
                            completed_at,
                        )
                        .await?,
                );
            }
        }

        if let Some(window) = window
            && window.status == ExecutionWindowStatus::Running
        {
            let Some((thread_id, workspace_id)) =
                self.crud_store.get_turn_location(turn_id).await?
            else {
                return self
                    .execution_window_continuation_admission_for_turn(turn_id, None)
                    .await;
            };
            let snapshot = self.crud_store.get_turn_runtime_snapshot(turn_id).await?;
            let binding = self
                .crud_store
                .get_cli_runtime_turn_binding(turn_id)
                .await?;
            let input = snapshot
                .as_ref()
                .and_then(|snapshot| {
                    serde_json::from_str::<Vec<UserInput>>(snapshot.input_json.as_str()).ok()
                })
                .unwrap_or_default();
            let model = snapshot
                .as_ref()
                .map(|snapshot| snapshot.model.clone())
                .or_else(|| binding.as_ref().and_then(|binding| binding.model.clone()))
                .unwrap_or_else(|| "native-cli".to_owned());
            let provider_name = snapshot
                .as_ref()
                .map(|snapshot| snapshot.provider_name.clone())
                .or_else(|| binding.as_ref().map(|binding| binding.runtime_kind.clone()))
                .unwrap_or_else(|| "native-cli".to_owned());

            self.checkpoint_running_execution_window(
                workspace_id.as_str(),
                thread_id.as_str(),
                turn_id,
                input.as_slice(),
                model.as_str(),
                provider_name.as_str(),
                now_unix,
                ExecutionWindowExhaustionReason::RuntimeShutdownContinuation,
                TurnExecutionCheckpointKind::WindowExhausted,
                interrupted_by,
                terminal_reason,
            )
            .await?;
        }

        self.execution_window_continuation_admission_for_turn(turn_id, None)
            .await
    }
}

fn checkpoint_kind_label(kind: pioneer_crud::TurnExecutionCheckpointKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

impl RecoveryCoordinator {
    async fn policy_for_timeout_candidate(
        &self,
        candidate: &TimeoutCandidate,
    ) -> Result<TimeoutRecoveryPolicyDecision> {
        let base_policy = self
            .policy_registry
            .policy_for_item_type(candidate.item_type);

        if !candidate.item_type.is_tool_item() {
            return Ok(TimeoutRecoveryPolicyDecision {
                policy: base_policy,
                policy_source: TimeoutRecoveryPolicySource::ItemTypeRegistry,
                tool_snapshot: None,
            });
        }

        let Some(item) = self
            .crud_store
            .get_turn_item(candidate.turn_id.as_str(), candidate.item_id.as_str())
            .await?
        else {
            return Ok(TimeoutRecoveryPolicyDecision {
                policy: conservative_missing_tool_snapshot_policy(base_policy),
                policy_source: TimeoutRecoveryPolicySource::ToolItemMissingSnapshot,
                tool_snapshot: None,
            });
        };

        if let Some(snapshot) = item.recovery_policy() {
            let mut policy = policy_from_tool_snapshot(snapshot);
            // The recovery metadata is descriptive; it is not an operation
            // ledger.  Until a tool is explicitly backed by a durable
            // idempotency record, replaying a partially completed
            // RequiresKey/SessionBound side effect is unsafe.  Preserve the
            // checkpoint and let the resumable path await reconciliation rather
            // than silently issuing the side effect twice.
            if matches!(
                snapshot.idempotency_mode,
                ToolRecoveryIdempotencyMode::RequiresKey
                    | ToolRecoveryIdempotencyMode::SessionBound
            ) {
                policy.action = RecoveryAction::BlockResumable;
                policy.max_attempts = 0;
                policy.base_backoff_secs = 0;
            }
            return Ok(TimeoutRecoveryPolicyDecision {
                policy,
                policy_source: TimeoutRecoveryPolicySource::ToolItemSnapshot,
                tool_snapshot: Some(snapshot.clone()),
            });
        }

        Ok(TimeoutRecoveryPolicyDecision {
            policy: conservative_missing_tool_snapshot_policy(base_policy),
            policy_source: TimeoutRecoveryPolicySource::ToolItemMissingSnapshot,
            tool_snapshot: None,
        })
    }

    async fn backfill_timeout_jobs(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<RecoveryCoordinatorEvent>> {
        let candidates = self
            .crud_store
            .list_unqueued_timeout_candidates(limit)
            .await?;
        let mut events = Vec::new();

        for candidate in candidates {
            let liveness = self
                .crud_store
                .get_turn_liveness(candidate.turn_id.as_str())
                .await?;
            if let TimeoutRecoveryClassification::SuppressRecoveryBecauseTurnProgressed {
                liveness,
            } = classify_timeout_candidate_liveness(&candidate, liveness.as_ref())
            {
                let context = timeout_recovery_suppression_context(&candidate, &liveness);
                let _ = self
                    .crud_store
                    .suppress_timeout_candidate_recovery(
                        &candidate,
                        TIMEOUT_RECOVERY_SUPPRESSED_TURN_PROGRESS,
                        context,
                        now_unix,
                    )
                    .await?;
                continue;
            }

            if self
                .suppress_timeout_recovery_if_turn_not_in_progress(&candidate, now_unix)
                .await?
            {
                continue;
            }

            if let Some((_job_id, active_events)) = self
                .record_recovery_timeout_failure(&candidate, now_unix)
                .await?
            {
                events.extend(active_events);
            } else {
                let outcome = self.enqueue_timeout_job(&candidate, now_unix).await?;
                events.push(recovery_enqueue_event_from_outcome(
                    &candidate,
                    RecoveryTrigger::Timeout,
                    outcome,
                ));
            }
        }

        Ok(events)
    }

    async fn policy_for_recovery_job(&self, job: &RecoveryJobRecord) -> Result<RecoveryPolicy> {
        Ok(RecoveryPolicy {
            action: job.action,
            max_attempts: job.max_attempts,
            base_backoff_secs: policy_snapshot_u64(job, "base_backoff_secs").unwrap_or(1),
            max_wall_clock_secs: policy_snapshot_u64(job, "max_wall_clock_secs").unwrap_or(60),
            no_progress_limit: policy_snapshot_i64(job, "no_progress_limit")
                .unwrap_or(job.max_attempts),
        })
    }

    async fn retained_provider_history_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Vec<RetainedProviderHistoryMessage>> {
        let rows = self
            .crud_store
            .list_turn_llm_context(turn_id)
            .await?
            .into_iter()
            .map(|row| RetainedProviderHistoryRow {
                sequence: row.sequence,
                source: row.source,
                item_id: row.item_id,
                tool_name: row.tool_name,
                payload: row.payload,
            })
            .collect();
        assemble_retained_provider_history(turn_id, rows)
    }

    async fn cancel_other_open_jobs_after_terminal_recovery(
        &self,
        turn_id: &str,
        terminal_job_id: &str,
        now_unix: i64,
    ) -> Result<()> {
        let _ = self
            .crud_store
            .cancel_open_recovery_jobs_for_turn(
                turn_id,
                Some(terminal_job_id),
                Some("terminal recovery outcome; pending recovery jobs cancelled".to_owned()),
                now_unix,
            )
            .await?;
        Ok(())
    }

    async fn active_recovery_exists_for_turn(&self, turn_id: &str) -> Result<bool> {
        Ok(!self
            .crud_store
            .find_recovery_jobs_by_turn_and_status(turn_id, RecoveryJobStatus::Active)
            .await?
            .is_empty())
    }

    async fn turn_accepts_recovery(&self, turn_id: &str) -> Result<bool> {
        let Some((thread_id, _workspace_id)) = self.crud_store.get_turn_location(turn_id).await?
        else {
            return Ok(false);
        };
        let Some((_workspace_id, turn)) = self
            .crud_store
            .get_turn(thread_id.as_str(), turn_id)
            .await?
        else {
            return Ok(false);
        };
        Ok(turn.status == TurnStatus::InProgress)
    }

    pub async fn suppress_timeout_recovery_if_turn_not_in_progress(
        &self,
        candidate: &TimeoutCandidate,
        now_unix: i64,
    ) -> Result<bool> {
        if self
            .turn_accepts_recovery(candidate.turn_id.as_str())
            .await?
        {
            return Ok(false);
        }

        let _ = self
            .crud_store
            .mark_attempt_recovery_action(
                candidate.attempt_id.as_str(),
                RecoveryAction::MarkFailed,
                now_unix,
            )
            .await?;
        Ok(true)
    }

    async fn open_recovery_for_turn(&self, turn_id: &str) -> Result<Option<RecoveryJobRecord>> {
        Ok(self
            .crud_store
            .find_open_recovery_jobs_for_turn(turn_id)
            .await?
            .into_iter()
            .next())
    }

    async fn build_attempt_plan(
        &self,
        job: &RecoveryJobRecord,
        attempt_number: u32,
    ) -> Result<RecoveryAttemptPlan> {
        if job.trigger != RecoveryTrigger::ProviderError {
            return Ok(RecoveryAttemptPlan::default());
        }

        let class = job.error_class.unwrap_or(ProviderFailureClass::Unknown);

        let mut plan = RecoveryAttemptPlan::default();

        match class {
            ProviderFailureClass::NetworkTransient
            | ProviderFailureClass::RateLimit
            | ProviderFailureClass::Provider5xx
            | ProviderFailureClass::EmptyResponse => {}
            ProviderFailureClass::StreamStall | ProviderFailureClass::StreamTruncated => {
                if attempt_number >= STREAM_TO_NON_STREAM_FALLBACK_ATTEMPT {
                    plan.force_non_stream = true;
                }
            }
            ProviderFailureClass::UnsupportedStreaming => {
                if provider_snapshot_field(job, "transport") == Some("stream") {
                    plan.force_non_stream = true;
                } else {
                    plan.terminal_reason = Some(
                        "provider rejected a non-stream request as unsupported streaming; no deterministic request change is available"
                            .to_owned(),
                    );
                }
            }
            ProviderFailureClass::AuthExpired
            | ProviderFailureClass::AuthOrPermission
            | ProviderFailureClass::PermissionDenied => {
                plan.refresh_provider_auth = true;
            }
            ProviderFailureClass::ModelNotFound => {
                plan.model_override = self.select_fallback_model(job).await?;
                if plan.model_override.is_none() {
                    plan.terminal_reason =
                        Some("model_not_found recovery has no fallback model".to_owned());
                }
            }
            ProviderFailureClass::PromptTooLong | ProviderFailureClass::ContextTooLarge => {
                plan.compact_history = true;
            }
            ProviderFailureClass::MaxOutputTokens => {
                plan.continue_generation = true;
            }
            ProviderFailureClass::UnsupportedParameter => {
                plan.terminal_reason = Some(
                    "provider rejected an unsupported parameter, but no adapter-supplied deterministic parameter removal is available"
                        .to_owned(),
                );
            }
            ProviderFailureClass::MalformedProviderRequest
            | ProviderFailureClass::ProviderRejected
            | ProviderFailureClass::InvalidRequest => {
                plan.terminal_reason = Some(
                    "provider rejected the request; unchanged invalid request will not be sent again"
                        .to_owned(),
                );
            }
            ProviderFailureClass::UnsupportedCapability => {
                plan.terminal_reason = Some(
                    "provider rejected an unspecified capability; no deterministic request change is available"
                        .to_owned(),
                );
            }
            ProviderFailureClass::UnsupportedToolCalling => {
                if provider_snapshot_field(job, "transport") == Some("stream") {
                    plan.force_non_stream = true;
                }
                plan.disable_tool_calling = true;
            }
            ProviderFailureClass::UnsupportedImageInput => {
                if provider_snapshot_field(job, "transport") == Some("stream") {
                    plan.force_non_stream = true;
                }
                plan.disable_image_input = true;
            }
            ProviderFailureClass::Unknown => {
                plan.terminal_reason = Some(
                    "provider failure is not classified as transient and has no deterministic recovery action"
                        .to_owned(),
                );
            }
        }

        Ok(plan)
    }

    async fn select_fallback_model(&self, job: &RecoveryJobRecord) -> Result<Option<String>> {
        if let Some(configured) = provider_snapshot_field(job, "fallback_model")
            && !configured.trim().is_empty()
        {
            return Ok(Some(configured.to_owned()));
        }

        Ok(None)
    }
}

fn provider_snapshot_field<'a>(job: &'a RecoveryJobRecord, key: &str) -> Option<&'a str> {
    job.policy_snapshot.get(key)?.as_str()
}

fn assemble_retained_provider_history(
    turn_id: &str,
    mut rows: Vec<RetainedProviderHistoryRow>,
) -> Result<Vec<RetainedProviderHistoryMessage>> {
    rows.sort_by_key(|row| row.sequence);

    let canonical_start = rows.iter().position(|row| {
        row.source == "tool_result_v2"
            || (row.source == "assistant_round"
                && serde_json::from_str::<pioneer_provider::CanonicalProviderRoundEnvelope>(
                    row.payload.as_str(),
                )
                .is_ok())
    });
    let Some(canonical_start) = canonical_start else {
        return assemble_legacy_provider_history(turn_id, rows);
    };

    let canonical_rows = rows.split_off(canonical_start);
    let mut retained = assemble_legacy_provider_history(turn_id, rows)?;
    retained.extend(assemble_canonical_provider_history(
        turn_id,
        canonical_rows,
    )?);
    Ok(retained)
}

fn assemble_legacy_provider_history(
    turn_id: &str,
    rows: Vec<RetainedProviderHistoryRow>,
) -> Result<Vec<RetainedProviderHistoryMessage>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut retained = Vec::with_capacity(rows.len());
    let mut pending_tool_calls = HashSet::<String>::new();
    let mut answered_tool_calls = HashSet::<String>::new();

    for row in rows {
        match row.source.as_str() {
            "assistant_round" => {
                if pending_tool_calls
                    .iter()
                    .any(|call_id| !answered_tool_calls.contains(call_id))
                {
                    bail!(
                        "retained provider history for turn `{turn_id}` starts a new assistant round before every prior tool call has a retained result"
                    );
                }

                let message =
                    serde_json::from_str::<ChatMessage>(row.payload.as_str()).map_err(|error| {
                        anyhow::anyhow!(
                            "invalid retained assistant round for turn `{turn_id}`: {error}"
                        )
                    })?;
                if message.role != Role::Assistant {
                    bail!(
                        "retained provider history for turn `{turn_id}` contains a non-assistant round"
                    );
                }
                let tool_calls = message.tool_calls.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "retained assistant round for turn `{turn_id}` has no tool calls"
                    )
                })?;
                if tool_calls.is_empty() {
                    bail!(
                        "retained assistant round for turn `{turn_id}` has an empty tool-call list"
                    );
                }

                pending_tool_calls.clear();
                answered_tool_calls.clear();
                for call in tool_calls {
                    if !pending_tool_calls.insert(call.id.clone()) {
                        bail!(
                            "retained assistant round for turn `{turn_id}` contains duplicate tool call `{}`",
                            call.id
                        );
                    }
                }
                retained.push(RetainedProviderHistoryMessage {
                    sequence: row.sequence,
                    message,
                });
            }
            "tool_result" => {
                let item_id = row.item_id.ok_or_else(|| {
                    anyhow::anyhow!(
                        "retained tool result for turn `{turn_id}` is missing its call id"
                    )
                })?;
                let tool_name = row.tool_name.ok_or_else(|| {
                    anyhow::anyhow!(
                        "retained tool result for turn `{turn_id}` call `{item_id}` is missing its tool name"
                    )
                })?;
                if !pending_tool_calls.contains(item_id.as_str()) {
                    bail!(
                        "retained tool result for turn `{turn_id}` call `{item_id}` has no preceding complete assistant round"
                    );
                }
                if !answered_tool_calls.insert(item_id.clone()) {
                    bail!(
                        "retained provider history for turn `{turn_id}` contains duplicate result for tool call `{item_id}`"
                    );
                }
                let view =
                    serde_json::from_str::<pioneer_tools::ToolResultView>(row.payload.as_str())?;
                retained.push(RetainedProviderHistoryMessage {
                    sequence: row.sequence,
                    message: recovered_tool_result_message(item_id, tool_name, view),
                });
            }
            source => {
                bail!(
                    "retained provider history for turn `{turn_id}` contains unsupported source `{source}`"
                );
            }
        }
    }

    if pending_tool_calls
        .iter()
        .any(|call_id| !answered_tool_calls.contains(call_id))
    {
        bail!(
            "retained provider history for turn `{turn_id}` is incomplete; refusing to send a malformed provider request"
        );
    }

    Ok(retained)
}

fn assemble_canonical_provider_history(
    turn_id: &str,
    rows: Vec<RetainedProviderHistoryRow>,
) -> Result<Vec<RetainedProviderHistoryMessage>> {
    use pioneer_provider::{CanonicalProviderRoundEnvelope, ProviderTermination};

    struct PendingRound {
        sequence: i64,
        envelope: CanonicalProviderRoundEnvelope,
        results: HashMap<String, ChatMessage>,
    }

    fn flush_round(
        turn_id: &str,
        pending: PendingRound,
        retained: &mut Vec<RetainedProviderHistoryMessage>,
    ) -> Result<()> {
        if pending.envelope.version != 1 {
            bail!(
                "retained provider round `{}` for turn `{turn_id}` has unsupported version {}",
                pending.envelope.round_id,
                pending.envelope.version
            );
        }
        if pending.envelope.round_id.trim().is_empty() {
            bail!("retained provider round for turn `{turn_id}` has an empty round identity");
        }
        if pending.envelope.message.role != Role::Assistant {
            bail!(
                "retained provider round `{}` for turn `{turn_id}` is not an assistant message",
                pending.envelope.round_id
            );
        }
        if pending.envelope.termination != ProviderTermination::ToolCalls {
            bail!(
                "retained provider round `{}` for turn `{turn_id}` has non-tool terminal semantics",
                pending.envelope.round_id
            );
        }

        let calls = pending
            .envelope
            .message
            .tool_calls
            .as_ref()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "retained provider round `{}` for turn `{turn_id}` has no tool calls",
                    pending.envelope.round_id
                )
            })?;
        if calls.len() != pending.envelope.calls.len() || calls.is_empty() {
            bail!(
                "retained provider round `{}` for turn `{turn_id}` has inconsistent call identity metadata",
                pending.envelope.round_id
            );
        }

        let mut identities = pending.envelope.calls.clone();
        identities.sort_by_key(|identity| identity.ordinal);
        let mut provider_ids = HashSet::new();
        let mut item_ids = HashSet::new();
        for (expected_ordinal, identity) in identities.iter().enumerate() {
            let expected_ordinal = u32::try_from(expected_ordinal).unwrap_or(u32::MAX);
            if identity.ordinal != expected_ordinal
                || identity.provider_call_id.trim().is_empty()
                || identity.turn_item_id.trim().is_empty()
                || !provider_ids.insert(identity.provider_call_id.clone())
                || !item_ids.insert(identity.turn_item_id.clone())
                || calls.get(expected_ordinal as usize).is_none_or(|call| {
                    call.id != identity.provider_call_id
                        || call.name.trim().is_empty()
                        || serde_json::from_str::<serde_json::Value>(call.arguments.as_str())
                            .is_err()
                })
            {
                bail!(
                    "retained provider round `{}` for turn `{turn_id}` has invalid or duplicate call identities",
                    pending.envelope.round_id
                );
            }
        }

        let missing = identities
            .iter()
            .filter(|identity| !pending.results.contains_key(identity.turn_item_id.as_str()))
            .map(|identity| identity.turn_item_id.clone())
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            bail!(
                "retained provider round `{}` for turn `{turn_id}` requires side-effect reconciliation; missing durable results for Turn items: {}",
                pending.envelope.round_id,
                missing.join(", ")
            );
        }

        retained.push(RetainedProviderHistoryMessage {
            sequence: pending.sequence,
            message: pending.envelope.message,
        });
        for (offset, identity) in identities.into_iter().enumerate() {
            let message = pending
                .results
                .get(identity.turn_item_id.as_str())
                .expect("missing results were checked")
                .clone();
            if message.role != Role::Tool
                || message.tool_call_id.as_deref() != Some(identity.provider_call_id.as_str())
            {
                bail!(
                    "retained result `{}` for provider round `{}` has mismatched provider identity",
                    identity.turn_item_id,
                    pending.envelope.round_id
                );
            }
            retained.push(RetainedProviderHistoryMessage {
                sequence: pending.sequence.saturating_add(offset as i64 + 1),
                message,
            });
        }
        Ok(())
    }

    let mut retained = Vec::new();
    let mut pending: Option<PendingRound> = None;
    for row in rows {
        match row.source.as_str() {
            "assistant_round" => {
                if let Some(previous) = pending.take() {
                    flush_round(turn_id, previous, &mut retained)?;
                }
                let envelope =
                    serde_json::from_str::<CanonicalProviderRoundEnvelope>(row.payload.as_str())
                        .map_err(|error| {
                            anyhow::anyhow!(
                                "invalid canonical provider round for turn `{turn_id}`: {error}"
                            )
                        })?;
                pending = Some(PendingRound {
                    sequence: row.sequence,
                    envelope,
                    results: HashMap::new(),
                });
            }
            "tool_result_v2" => {
                let pending = pending.as_mut().ok_or_else(|| anyhow::anyhow!(
                    "exact provider result for turn `{turn_id}` has no preceding canonical round"
                ))?;
                let item_id = row.item_id.ok_or_else(|| anyhow::anyhow!(
                    "exact provider result for turn `{turn_id}` is missing its Turn item identity"
                ))?;
                let view =
                    serde_json::from_str::<pioneer_tools::ToolResultView>(row.payload.as_str())?;
                let pioneer_tools::ToolResultView::Json {
                    value,
                    truncated: false,
                } = view
                else {
                    bail!(
                        "exact provider result `{item_id}` for turn `{turn_id}` is not an untruncated canonical message"
                    );
                };
                let message = serde_json::from_value::<ChatMessage>(value).map_err(|error| {
                    anyhow::anyhow!(
                        "invalid exact provider result `{item_id}` for turn `{turn_id}`: {error}"
                    )
                })?;
                if pending.results.insert(item_id.clone(), message).is_some() {
                    bail!(
                        "canonical provider round `{}` for turn `{turn_id}` has duplicate result for Turn item `{item_id}`",
                        pending.envelope.round_id
                    );
                }
            }
            source => bail!(
                "canonical provider history for turn `{turn_id}` contains incompatible source `{source}`"
            ),
        }
    }
    if let Some(pending) = pending {
        flush_round(turn_id, pending, &mut retained)?;
    }
    Ok(retained)
}

fn recovered_tool_result_message(
    item_id: String,
    tool_name: String,
    view: pioneer_tools::ToolResultView,
) -> ChatMessage {
    let (content, payload) = match view {
        pioneer_tools::ToolResultView::Text { text, truncated } => (
            text.clone(),
            serde_json::json!({
                "output": text,
                "truncated": truncated,
                "recovered_from_turn_llm_context": true,
            }),
        ),
        pioneer_tools::ToolResultView::Json {
            mut value,
            truncated,
        } => {
            if !value.is_object() {
                value = serde_json::json!({ "value": value });
            }
            if let Some(map) = value.as_object_mut() {
                map.entry("truncated".to_owned())
                    .or_insert(serde_json::Value::Bool(truncated));
                map.insert(
                    "recovered_from_turn_llm_context".to_owned(),
                    serde_json::Value::Bool(true),
                );
            }
            let content =
                serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
            (content, value)
        }
        pioneer_tools::ToolResultView::Empty => (
            String::new(),
            serde_json::json!({
                "recovered_from_turn_llm_context": true
            }),
        ),
    };

    ModelInputItem::tool_result(item_id, tool_name, content, Some(payload)).into_chat_message()
}

fn policy_snapshot_i64(job: &RecoveryJobRecord, key: &str) -> Option<i64> {
    let value = job
        .policy_snapshot
        .get(key)
        .or_else(|| job.policy_json.get(key))?;
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn policy_snapshot_u64(job: &RecoveryJobRecord, key: &str) -> Option<u64> {
    let value = job
        .policy_snapshot
        .get(key)
        .or_else(|| job.policy_json.get(key))?;
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
}

fn recovery_enqueue_event_from_outcome(
    candidate: &TimeoutCandidate,
    trigger: RecoveryTrigger,
    outcome: RecoveryJobEnqueueOutcome,
) -> RecoveryCoordinatorEvent {
    let next_attempt_number = outcome.next_attempt_number();
    match outcome {
        RecoveryJobEnqueueOutcome::Created(job) => RecoveryCoordinatorEvent::RecoveryOpened {
            job_id: job.id,
            turn_id: candidate.turn_id.clone(),
            item_id: candidate.item_id.clone(),
            item_type: candidate.item_type,
            trigger: job.trigger,
            action: job.action,
            attempt_number: next_attempt_number,
        },
        RecoveryJobEnqueueOutcome::Reused(job) => RecoveryCoordinatorEvent::RecoveryAttached {
            job_id: job.id,
            turn_id: candidate.turn_id.clone(),
            item_id: candidate.item_id.clone(),
            item_type: candidate.item_type,
            recovery_item_id: job.item_id,
            recovery_item_type: job.item_type,
            trigger,
            action: job.action,
            existing_status: job.status,
            next_attempt_number,
        },
    }
}

fn policy_from_tool_snapshot(snapshot: &ToolRecoveryPolicySnapshot) -> RecoveryPolicy {
    RecoveryPolicy {
        action: snapshot.resolved_action,
        max_attempts: i64::from(snapshot.max_attempts),
        base_backoff_secs: snapshot.base_backoff_secs,
        max_wall_clock_secs: snapshot.max_wall_clock_secs,
        no_progress_limit: snapshot.no_progress_limit,
    }
}

fn conservative_missing_tool_snapshot_policy(mut base: RecoveryPolicy) -> RecoveryPolicy {
    base.action = RecoveryAction::BlockResumable;
    base.max_attempts = 0;
    base.base_backoff_secs = 0;
    base.no_progress_limit = 0;
    base
}

fn recovery_trigger_name(trigger: RecoveryTrigger) -> &'static str {
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

fn policy_action_name(action: RecoveryAction) -> &'static str {
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

pub(crate) fn provider_failure_class_name(class: ProviderFailureClass) -> &'static str {
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

fn terminal_policy_error_message(job: &RecoveryJobRecord) -> String {
    const BASE: &str = "recovery policy marks this failure as terminal";
    match job
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
    {
        Some(reason) => format!("{BASE}: {reason}"),
        None => BASE.to_owned(),
    }
}

fn block_resumable_policy_message(job: &RecoveryJobRecord) -> String {
    let base = match job.error_class {
        Some(class) => format!(
            "recovery is blocked and requires user/operator action: {}",
            provider_failure_class_name(class)
        ),
        None => format!(
            "recovery is blocked and requires user/operator action: {}",
            recovery_trigger_name(job.trigger)
        ),
    };
    match job
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
    {
        Some(reason) => format!("{base}: {reason}"),
        None => base,
    }
}

fn tool_retry_class_name(retry_class: ToolRecoveryRetryClass) -> &'static str {
    match retry_class {
        ToolRecoveryRetryClass::Never => "never",
        ToolRecoveryRetryClass::Transient => "transient",
        ToolRecoveryRetryClass::Arguments => "arguments",
        ToolRecoveryRetryClass::Session => "session",
        ToolRecoveryRetryClass::Network => "network",
    }
}

fn tool_idempotency_mode_name(idempotency_mode: ToolRecoveryIdempotencyMode) -> &'static str {
    match idempotency_mode {
        ToolRecoveryIdempotencyMode::None => "none",
        ToolRecoveryIdempotencyMode::Safe => "safe",
        ToolRecoveryIdempotencyMode::RequiresKey => "requires_key",
        ToolRecoveryIdempotencyMode::SessionBound => "session_bound",
    }
}

fn backoff_deadline(
    now_unix: i64,
    base_backoff_secs: u64,
    run_count: i64,
    seed: &str,
    retry_after_ms: Option<i64>,
) -> i64 {
    if let Some(retry_after_ms) = retry_after_ms {
        let retry_secs = retry_after_ms.saturating_add(999).saturating_div(1000);
        return now_unix.saturating_add(retry_secs.max(1));
    }

    let exponent = u32::try_from(run_count.max(0)).unwrap_or(0).min(8);
    let multiplier = 1u64.checked_shl(exponent).unwrap_or(u64::MAX);
    let base_delay = base_backoff_secs.max(1).saturating_mul(multiplier);
    let jitter = deterministic_jitter_secs(seed, base_delay);
    let delay = base_delay.saturating_add(jitter);
    now_unix.saturating_add(i64::try_from(delay).unwrap_or(i64::MAX))
}

fn attempt_number_for_job(job: &RecoveryJobRecord) -> u32 {
    if job.trigger == RecoveryTrigger::ProviderError {
        u32::try_from(job.provider_attempt_number.saturating_add(1)).unwrap_or(u32::MAX)
    } else {
        u32::try_from(job.run_count.saturating_add(1).max(0)).unwrap_or(u32::MAX)
    }
}

fn deterministic_jitter_secs(seed: &str, base_delay_secs: u64) -> u64 {
    let hash = seed.bytes().fold(0u64, |acc, value| {
        acc.wrapping_mul(131).wrapping_add(value as u64)
    });
    let spread = (base_delay_secs / 4).max(1);
    hash % spread
}

#[cfg(test)]
mod tests {
    use super::{
        ProviderFailureCandidate, RecoveryCoordinator, RecoveryCoordinatorEvent,
        RecoveryJobEnqueueOutcome, RecoveryPolicyRegistry, RetainedProviderHistoryRow,
        RuntimeFailureCandidate, TURN_RECOVERY_MAX_WALL_CLOCK_SECS,
        assemble_retained_provider_history, is_causal_recovery_progress,
    };
    use migration::{Migrator, MigratorTrait};
    use pioneer_agent::{
        AgentManager, AgentTurnHookRuntimeContext, ResolvedArtifactInput,
        SkillsDependenciesLoopConfig, SkillsLoopConfig, SkillsRuntimeLoopConfig,
        SkillsSecurityLoopConfig, SkillsValidationLoopConfig, ToolLoopConfig, WorkspaceSkillPolicy,
    };
    use pioneer_crud::{
        ClaimedRecoveryActivation, CrudStore, NewCliRuntimeTurnBinding,
        NewTurnExecutionCheckpointRecord, NewTurnExecutionWindowRecord, NewTurnLlmContextEntry,
        NewTurnRuntimeSnapshot, RecoveryJobRecord, SkillInstallationRecord,
        SkillPackInstallationRecord, TimeoutCandidate, TurnExecutionCheckpointKind,
        TurnExecutionWindowStatsRecord,
    };
    use pioneer_entity::{turn, workspace};
    use pioneer_protocol::{
        AgentMessagePhase, ExecutionWindowExhaustionReason, ExecutionWindowStatus,
        ItemCompletedNotification, ItemStartedNotification, ProviderFailureClass,
        ProviderFailureDetails, ProviderFailureStage, ProviderTransportKind, RecoveryAction,
        RecoveryAttemptContext, RecoveryJobStatus, RecoveryTrigger, SandboxMode, Thread,
        ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus, ToolCallStatus,
        ToolDisplayPayload, ToolOutputPolicySnapshot, ToolRecoveryIdempotencyMode,
        ToolRecoveryPolicySnapshot, ToolRecoveryRetryClass, ToolStoragePayload, TurnCapability,
        TurnCapabilityKind, TurnCompletedNotification, TurnExecutionSecuritySnapshot,
        TurnFilesystemAccess, TurnFilesystemSandboxEntry, TurnItem, TurnItemTimeoutReason,
        TurnItemType, TurnPermissionMode, TurnPermissionProfileSnapshot,
        TurnPermissionProfileSource, TurnSandboxMode, TurnStatus, UserInput,
        build_execution_checkpoint_payload,
    };
    use pioneer_provider::{
        CanonicalProviderRoundEnvelope, ChatMessage, InputContentType, MessageAttachment,
        ProviderCallIdentity, ProviderRegistry, ProviderReplayState, ProviderTermination,
        ProviderToolCall, providers::EchoProvider,
    };
    use pioneer_skills::{SkillPolicyKey, SkillTrustLevel};
    use pioneer_tools::{
        ComputerUseToolsConfig, ExecutionWindowsConfig, ToolLoopBudgetConfig,
        ToolRetryBudgetConfig, WebToolsConfig,
    };
    use sea_orm::{ActiveModelTrait, ConnectionTrait, Database, EntityTrait, Set};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn retained_history_row(
        sequence: i64,
        source: &str,
        item_id: Option<&str>,
        tool_name: Option<&str>,
        payload: String,
    ) -> RetainedProviderHistoryRow {
        RetainedProviderHistoryRow {
            sequence,
            source: source.to_owned(),
            item_id: item_id.map(str::to_owned),
            tool_name: tool_name.map(str::to_owned),
            payload,
        }
    }

    #[test]
    fn retained_provider_history_replays_complete_assistant_round_losslessly() {
        let assistant = ChatMessage::assistant_tool_calls_with_provider_state(
            Some("working"),
            Some("private reasoning"),
            vec![
                ProviderToolCall {
                    id: "call_a".to_owned(),
                    name: "read_file".to_owned(),
                    arguments: r#"{"path":"a"}"#.to_owned(),
                },
                ProviderToolCall {
                    id: "call_b".to_owned(),
                    name: "read_file".to_owned(),
                    arguments: r#"{"path":"b"}"#.to_owned(),
                },
            ],
            Some(ProviderReplayState::new(
                "anthropic",
                serde_json::json!({
                    "blocks": [{
                        "type": "thinking",
                        "thinking": "private reasoning",
                        "signature": "signed-state"
                    }]
                }),
            )),
        );
        let rows = vec![
            retained_history_row(
                12,
                "tool_result",
                Some("call_a"),
                Some("read_file"),
                serde_json::to_string(&pioneer_tools::ToolResultView::Text {
                    text: "a result".to_owned(),
                    truncated: false,
                })
                .unwrap(),
            ),
            retained_history_row(
                10,
                "assistant_round",
                None,
                None,
                serde_json::to_string(&assistant).unwrap(),
            ),
            retained_history_row(
                11,
                "tool_result",
                Some("call_b"),
                Some("read_file"),
                serde_json::to_string(&pioneer_tools::ToolResultView::Text {
                    text: "b result".to_owned(),
                    truncated: false,
                })
                .unwrap(),
            ),
        ];

        let retained = assemble_retained_provider_history("turn_lossless", rows).unwrap();

        assert_eq!(retained.len(), 3);
        assert_eq!(retained[0].message, assistant);
        assert_eq!(retained[1].message.tool_call_id.as_deref(), Some("call_b"));
        assert_eq!(retained[2].message.tool_call_id.as_deref(), Some("call_a"));
    }

    #[test]
    fn retained_provider_history_rejects_legacy_tool_result_without_assistant_round() {
        let rows = vec![retained_history_row(
            1,
            "tool_result",
            Some("call_legacy"),
            Some("read_file"),
            serde_json::to_string(&pioneer_tools::ToolResultView::Empty).unwrap(),
        )];

        let error = assemble_retained_provider_history("turn_legacy", rows).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("no preceding complete assistant round")
        );
    }

    #[test]
    fn retained_provider_history_rejects_incomplete_parallel_tool_results() {
        let assistant = ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![
                ProviderToolCall {
                    id: "call_a".to_owned(),
                    name: "read_file".to_owned(),
                    arguments: "{}".to_owned(),
                },
                ProviderToolCall {
                    id: "call_b".to_owned(),
                    name: "read_file".to_owned(),
                    arguments: "{}".to_owned(),
                },
            ],
        );
        let rows = vec![
            retained_history_row(
                1,
                "assistant_round",
                None,
                None,
                serde_json::to_string(&assistant).unwrap(),
            ),
            retained_history_row(
                2,
                "tool_result",
                Some("call_a"),
                Some("read_file"),
                serde_json::to_string(&pioneer_tools::ToolResultView::Empty).unwrap(),
            ),
        ];

        let error = assemble_retained_provider_history("turn_incomplete", rows).unwrap_err();

        assert!(error.to_string().contains("is incomplete"));
    }

    fn canonical_round_row(
        sequence: i64,
        round_id: &str,
        provider_call_id: &str,
        item_id: &str,
    ) -> RetainedProviderHistoryRow {
        let message = ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: provider_call_id.to_owned(),
                name: "read_file".to_owned(),
                arguments: "{}".to_owned(),
            }],
        );
        retained_history_row(
            sequence,
            "assistant_round",
            Some(round_id),
            None,
            serde_json::to_string(&CanonicalProviderRoundEnvelope {
                version: 1,
                round_id: round_id.to_owned(),
                termination: ProviderTermination::ToolCalls,
                message,
                calls: vec![ProviderCallIdentity {
                    provider_call_id: provider_call_id.to_owned(),
                    turn_item_id: item_id.to_owned(),
                    ordinal: 0,
                }],
            })
            .unwrap(),
        )
    }

    fn exact_result_row(
        sequence: i64,
        item_id: &str,
        provider_call_id: &str,
        text: &str,
    ) -> RetainedProviderHistoryRow {
        let message = ChatMessage::tool_result(provider_call_id, "read_file", text);
        retained_history_row(
            sequence,
            "tool_result_v2",
            Some(item_id),
            Some("read_file"),
            serde_json::to_string(&pioneer_tools::ToolResultView::Json {
                value: serde_json::to_value(message).unwrap(),
                truncated: false,
            })
            .unwrap(),
        )
    }

    #[test]
    fn canonical_provider_history_replays_exact_result_by_provider_identity() {
        let retained = assemble_retained_provider_history(
            "turn_v2",
            vec![
                canonical_round_row(1, "round_1", "provider_call", "turn_item_1"),
                exact_result_row(2, "turn_item_1", "provider_call", "exact output"),
            ],
        )
        .unwrap();

        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].message.role, pioneer_provider::Role::Assistant);
        assert_eq!(retained[1].message.role, pioneer_provider::Role::Tool);
        assert_eq!(
            retained[1].message.tool_call_id.as_deref(),
            Some("provider_call")
        );
        assert_eq!(retained[1].message.content, "exact output");
    }

    #[test]
    fn canonical_provider_history_reports_reconciliation_for_partial_round() {
        let error = assemble_retained_provider_history(
            "turn_partial",
            vec![canonical_round_row(
                1,
                "round_partial",
                "provider_call",
                "turn_item_missing",
            )],
        )
        .unwrap_err();

        let message = error.to_string();
        assert!(message.contains("requires side-effect reconciliation"));
        assert!(message.contains("turn_item_missing"));
    }

    #[test]
    fn repeated_provider_call_ids_across_rounds_keep_distinct_turn_items() {
        let retained = assemble_retained_provider_history(
            "turn_reused_provider_id",
            vec![
                canonical_round_row(1, "round_1", "call_1", "turn_item_1"),
                exact_result_row(2, "turn_item_1", "call_1", "first"),
                canonical_round_row(3, "round_2", "call_1", "turn_item_2"),
                exact_result_row(4, "turn_item_2", "call_1", "second"),
            ],
        )
        .unwrap();

        assert_eq!(retained.len(), 4);
        assert_eq!(retained[1].message.content, "first");
        assert_eq!(retained[3].message.content, "second");
        assert_eq!(retained[1].message.tool_call_id.as_deref(), Some("call_1"));
        assert_eq!(retained[3].message.tool_call_id.as_deref(), Some("call_1"));
    }

    #[test]
    fn completed_legacy_prefix_can_continue_with_canonical_rounds_after_upgrade() {
        let legacy_assistant = ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: "legacy_call".to_owned(),
                name: "read_file".to_owned(),
                arguments: "{}".to_owned(),
            }],
        );
        let retained = assemble_retained_provider_history(
            "turn_upgraded",
            vec![
                retained_history_row(
                    1,
                    "assistant_round",
                    Some("legacy_round"),
                    None,
                    serde_json::to_string(&legacy_assistant).unwrap(),
                ),
                retained_history_row(
                    2,
                    "tool_result",
                    Some("legacy_call"),
                    Some("read_file"),
                    serde_json::to_string(&pioneer_tools::ToolResultView::Text {
                        text: "legacy result".to_owned(),
                        truncated: false,
                    })
                    .unwrap(),
                ),
                canonical_round_row(3, "round_v2", "provider_call", "turn_item_v2"),
                exact_result_row(4, "turn_item_v2", "provider_call", "canonical result"),
            ],
        )
        .unwrap();

        assert_eq!(retained.len(), 4);
        let legacy_payload =
            serde_json::from_str::<serde_json::Value>(&retained[1].message.content).unwrap();
        assert_eq!(legacy_payload["output"], "legacy result");
        assert_eq!(legacy_payload["truncated"], false);
        assert_eq!(legacy_payload["recovered_from_turn_llm_context"], true);
        assert_eq!(
            retained[1].message.tool_call_id.as_deref(),
            Some("legacy_call")
        );
        assert_eq!(retained[3].message.content, "canonical result");
    }

    #[test]
    fn command_execution_recovery_wall_clock_uses_config() {
        let registry = RecoveryPolicyRegistry::with_command_execution_timeout_config(
            pioneer_config::GatewayCommandExecutionTimeoutConfig {
                lease_secs: 60,
                idle_secs: 120,
                hard_secs: 300,
                recovery_max_wall_clock_secs: 900,
            },
        );

        let policy = registry.policy_for_item_type(TurnItemType::CommandExecution);

        assert_eq!(policy.max_wall_clock_secs, 900);
    }

    #[test]
    fn recoverable_provider_failures_use_fifteen_minute_job_budget() {
        let registry = RecoveryPolicyRegistry::default();

        for class in [
            ProviderFailureClass::NetworkTransient,
            ProviderFailureClass::RateLimit,
            ProviderFailureClass::Provider5xx,
        ] {
            assert_eq!(
                registry
                    .policy_for_provider_failure(class)
                    .max_wall_clock_secs,
                TURN_RECOVERY_MAX_WALL_CLOCK_SECS
            );
        }
    }

    fn test_tool_loop_config() -> ToolLoopConfig {
        ToolLoopConfig {
            provider: pioneer_provider::ProviderTimeoutPolicy::default(),
            preflight: pioneer_agent::PreflightLoopConfig::default(),
            web: WebToolsConfig {
                default_timeout_ms: 20_000,
                hard_max_timeout_ms: 120_000,
                default_fetch_max_bytes: 2 * 1024 * 1024,
                hard_fetch_max_bytes: 8 * 1024 * 1024,
                default_download_max_bytes: 128 * 1024 * 1024,
                hard_download_max_bytes: 1024 * 1024 * 1024,
                default_max_results: 8,
                hard_max_results: 20,
                default_snippet_chars: 420,
                hard_max_snippet_chars: 4_096,
                default_link_count: 40,
                hard_link_count: 200,
                default_render_max_chars: 40_000,
                ddg_html_search_url: "https://duckduckgo.com/html/".to_owned(),
                ddg_instant_api_url: "https://api.duckduckgo.com/".to_owned(),
                default_user_agent: "Mozilla/5.0".to_owned(),
            },
            computer_use: ComputerUseToolsConfig::default(),
            skills: SkillsLoopConfig {
                enabled: true,
                max_skills_per_source: 256,
                max_skill_file_bytes: 1024 * 1024,
                prompt_max_chars: 24_000,
                allow_implicit_invocation: false,
                system_roots: Vec::new(),
                user_roots: vec!["/tmp/pioneer-recovery-user-skills".to_owned()],
                registry_roots: vec!["/tmp/pioneer-recovery-registry-skills".to_owned()],
                system_import_roots: Vec::new(),
                user_import_roots: Vec::new(),
                registry_import_roots: Vec::new(),
                validation: SkillsValidationLoopConfig {
                    strict_agentskills: true,
                    accept_openclaw_profile: true,
                },
                security: SkillsSecurityLoopConfig {
                    allow_untrusted_install: false,
                    min_trust_for_shell_tools: SkillTrustLevel::Verified,
                    min_trust_for_http_tools: SkillTrustLevel::Community,
                    min_trust_for_function_proxy_tools: SkillTrustLevel::Community,
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
            memory: pioneer_memory::hooks::MemoryLoopConfig {
                active_recall: pioneer_memory::hooks::MemoryActiveRecallConfig {
                    mode: pioneer_memory::hooks::MemoryActiveRecallMode::DeterministicOnly,
                    ..pioneer_memory::hooks::MemoryActiveRecallConfig::default()
                },
                ..pioneer_memory::hooks::MemoryLoopConfig::default()
            },
            budget: ToolLoopBudgetConfig::default(),
            execution_windows: ExecutionWindowsConfig::default(),
            retry: ToolRetryBudgetConfig::default(),
        }
    }

    async fn setup_coordinator_with_tool_loop_config(
        tool_loop_config: ToolLoopConfig,
    ) -> (Arc<CrudStore>, Arc<AgentManager>, RecoveryCoordinator) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        let crud_store = Arc::new(CrudStore::new(connection));
        ensure_recovery_test_execution_authority(crud_store.as_ref()).await;
        let provider_registry = Arc::new(ProviderRegistry::with_provider(
            "echo",
            Arc::new(EchoProvider::new()),
        ));
        let agent_manager = Arc::new(AgentManager::new(
            provider_registry.clone(),
            tool_loop_config.clone(),
        ));
        let coordinator = RecoveryCoordinator::new(
            crud_store.clone(),
            agent_manager.clone(),
            provider_registry,
            RecoveryPolicyRegistry::default(),
            tool_loop_config.normalized(),
        );
        let listener_manager = agent_manager.clone();
        coordinator
            .set_listener_starter(Arc::new(move |thread_id| {
                let listener_manager = listener_manager.clone();
                Box::pin(async move {
                    let Some(mut receiver) = listener_manager
                        .take_durable_receiver(thread_id.as_str())
                        .await
                    else {
                        // A prior recovery attempt already owns the single
                        // durable receiver for this test thread.
                        return Ok(());
                    };
                    tokio::spawn(async move {
                        while receiver.recv().await.is_some() {
                            receiver.acknowledge_last(Ok(()));
                        }
                    });
                    Ok(())
                })
            }))
            .await;
        (crud_store, agent_manager, coordinator)
    }

    async fn ensure_recovery_test_execution_authority(crud_store: &CrudStore) {
        crud_store
            .database_connection()
            .execute_unprepared(
                "INSERT OR IGNORE INTO gateway_identity(\
                    id,singleton_key,identity_bootstrap_version,auth_schema_version,auth_ready_at,\
                    created_at,updated_at\
                 ) VALUES(\
                    'G00000000000000000001',1,1,2,CURRENT_TIMESTAMP,\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP\
                 );\
                 INSERT OR IGNORE INTO gateway_principal(\
                    id,gateway_id,kind,role_key,status,display_name,nickname,nickname_key,\
                    created_at,updated_at,removed_at\
                 ) VALUES(\
                    'P00000000000000000001','G00000000000000000001','superuser',NULL,'active',\
                    'Superuser','superuser','superuser',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,NULL\
                 );\
                 INSERT OR IGNORE INTO device(\
                    id,gateway_id,principal_id,installation_id,display_name,client_kind,\
                    platform,client_version,status,created_at,updated_at,last_seen_at,revoked_at\
                 ) VALUES(\
                    'D00000000000000000001','G00000000000000000001',\
                    'P00000000000000000001','recovery-test-superuser','Recovery Test Superuser',\
                    'desktop','test','1','active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,\
                    CURRENT_TIMESTAMP,NULL\
                 );\
                 INSERT OR IGNORE INTO auth_session(\
                    id,gateway_id,principal_id,device_id,token_family_id,created_by_session_id,\
                    activation_token_hash,activation_locator_hash,activation_failed_attempts,\
                    activation_expires_at,activated_at,status,refresh_generation,created_at,\
                    updated_at,last_seen_at,last_refreshed_at,refresh_expires_at,revoked_at,\
                    revoke_reason\
                 ) VALUES(\
                    'S00000000000000000001','G00000000000000000001',\
                    'P00000000000000000001','D00000000000000000001',\
                    'F00000000000000000001',NULL,randomblob(32),randomblob(32),0,\
                    datetime('now','+10 minutes'),CURRENT_TIMESTAMP,'active',0,\
                    CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,\
                    datetime('now','+90 days'),NULL,NULL\
                 );\
                 INSERT OR IGNORE INTO auth_refresh_credential(\
                    id,session_id,token_family_id,generation,token_hash,issued_at,expires_at\
                 ) VALUES(\
                    'R00000000000000000001','S00000000000000000001',\
                    'F00000000000000000001',0,randomblob(32),CURRENT_TIMESTAMP,\
                    datetime('now','+90 days')\
                 );",
            )
            .await
            .expect("recovery test execution authority should materialize");
    }

    async fn setup_coordinator_with_agent()
    -> (Arc<CrudStore>, Arc<AgentManager>, RecoveryCoordinator) {
        setup_coordinator_with_tool_loop_config(test_tool_loop_config()).await
    }

    async fn setup_coordinator() -> (Arc<CrudStore>, RecoveryCoordinator) {
        let (crud_store, _agent_manager, coordinator) = setup_coordinator_with_agent().await;
        (crud_store, coordinator)
    }

    fn provider_failure(class: ProviderFailureClass, message: &str) -> ProviderFailureDetails {
        ProviderFailureDetails {
            provider: "test-provider".to_owned(),
            model: "test-model".to_owned(),
            transport: ProviderTransportKind::NonStream,
            class,
            stage: ProviderFailureStage::Finalize,
            http_status: None,
            provider_code: None,
            retry_after_ms: None,
            is_recoverable_hint: true,
            message: Some(message.to_owned()),
        }
    }

    fn timeout_candidate(
        attempt_id: &str,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
    ) -> TimeoutCandidate {
        TimeoutCandidate {
            attempt_id: attempt_id.to_owned(),
            turn_id: turn_id.to_owned(),
            item_id: item_id.to_owned(),
            item_type,
            execution_class: pioneer_protocol::TurnItemExecutionClass::Standard,
            attempt_number: 1,
            timeout_reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
            started_at_unix: 1_700_000_000,
            started_event_sequence: Some(2),
            last_heartbeat_at_unix: Some(1_700_000_000),
            lease_expires_at_unix: Some(1_700_000_001),
            idle_deadline_at_unix: Some(1_700_000_001),
            hard_deadline_at_unix: Some(1_700_000_001),
        }
    }

    fn provider_plan_job(class: ProviderFailureClass, transport: &str) -> RecoveryJobRecord {
        RecoveryJobRecord {
            id: format!("job_plan_{}", super::provider_failure_class_name(class)),
            turn_id: "turn_plan".to_owned(),
            item_id: "reasoning_plan".to_owned(),
            item_type: TurnItemType::Reasoning,
            source_attempt_id: None,
            status: RecoveryJobStatus::Pending,
            trigger: RecoveryTrigger::ProviderError,
            action: RecoveryAction::RetryWithBackoff,
            reason: None,
            error_class: Some(class),
            transport_stage: Some(ProviderFailureStage::Finalize),
            retry_after_ms: None,
            provider_attempt_number: 1,
            policy_json: serde_json::json!({}),
            policy_snapshot: serde_json::json!({ "transport": transport }),
            last_error: None,
            run_count: 0,
            max_attempts: 2,
            scheduled_at_unix: 1_700_000_000,
            next_run_at_unix: 1_700_000_000,
            updated_at_unix: 1_700_000_000,
            claim_token: None,
            active_attempt_id: None,
            active_attempt_started_at_unix: None,
        }
    }

    fn tool_snapshot(
        retry_class: ToolRecoveryRetryClass,
        idempotency_mode: ToolRecoveryIdempotencyMode,
        max_attempts: u8,
        action: RecoveryAction,
    ) -> ToolRecoveryPolicySnapshot {
        ToolRecoveryPolicySnapshot {
            retry_class,
            idempotency_mode,
            max_attempts,
            can_resume: true,
            resolved_action: action,
            base_backoff_secs: 7,
            max_wall_clock_secs: 777,
            no_progress_limit: 9,
        }
    }

    async fn materialize_turn_with_tool_item(
        crud_store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
    ) {
        materialize_turn_with_tool_item_and_permission_profile(
            crud_store,
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            pioneer_protocol::default_turn_permission_profile_snapshot(),
            recovery_policy,
        )
        .await;
    }

    async fn materialize_turn_with_tool_item_and_permission_profile(
        crud_store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
        item_id: &str,
        permission_profile: TurnPermissionProfileSnapshot,
        recovery_policy: Option<ToolRecoveryPolicySnapshot>,
    ) {
        crate::session::test_support::ensure_test_superuser_execution_authority(crud_store).await;
        crate::workspace::WorkspaceManager::new(crud_store.database_connection())
            .create_workspace(workspace_id, Some("Recovery test workspace"))
            .await
            .expect("recovery test workspace should materialize");
        let timestamp = 1_700_000_000;
        let thread = Thread {
            workspace_id: workspace_id.to_owned(),
            id: thread_id.to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "test-model".to_owned(),
            model_provider: "echo".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = pioneer_protocol::Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile,
        };

        let principal = crate::session::test_support::authenticated_test_superuser();
        crud_store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[UserInput::Text {
                    text: "run tool".to_owned(),
                    text_elements: Vec::new(),
                }],
                pioneer_protocol::PersistedActorRef::Principal(principal.principal_id.clone()),
            )
            .await
            .expect("turn start should persist");
        let persisted = turn::Entity::find_by_id(turn_id)
            .one(&crud_store.database_connection())
            .await
            .expect("recovery turn provenance query should succeed")
            .expect("recovery turn should exist");
        assert_eq!(
            persisted.initiated_by_actor_kind.as_deref(),
            Some("principal"),
            "resilience recovery fixtures must retain initiating authority"
        );
        assert_eq!(
            persisted.initiated_by_actor_id.as_deref(),
            Some(principal.principal_id.as_str())
        );
        let context = crate::authorization::ExecutionAuthorizationContext::for_test(
            principal.as_ref(),
            workspace_id,
            thread_id,
            &turn.permission_profile,
            None,
        );
        let encoded = context
            .to_persisted_json()
            .expect("recovery test execution authorization should serialize");
        assert!(
            crud_store
                .set_turn_execution_authorization_context(turn_id, encoded.as_str())
                .await
                .expect("recovery test execution authorization should persist")
        );
        crud_store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    item: TurnItem::WebFetch {
                        id: item_id.to_owned(),
                        tool_name: "web_fetch".to_owned(),
                        arguments: serde_json::json!({"url": "https://example.com"}),
                        status: ToolCallStatus::InProgress,
                        recovery_policy,
                        output_policy: ToolOutputPolicySnapshot::for_tool_name("web_fetch"),
                        display: ToolDisplayPayload::Hidden,
                        storage: ToolStoragePayload::Metadata {
                            metadata: pioneer_protocol::ToolMetadata::from_json(
                                serde_json::json!({
                                    "url": "https://example.com"
                                }),
                            ),
                        },
                        recovery: None,
                        url: Some("https://example.com".to_owned()),
                        final_url: None,
                        status_code: None,
                        content_type: None,
                        extract_mode: None,
                        resolved_mode: None,
                        bytes_received: None,
                        elapsed_ms: None,
                        truncated: None,
                        title: None,
                        word_count: None,
                        links: Vec::new(),
                        success: None,
                        outcome: None,
                        observation: None,
                    },
                },
                timestamp + 1,
            )
            .await
            .expect("tool item should persist");
    }

    async fn persist_test_runtime_snapshot(
        crud_store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) {
        persist_test_runtime_snapshot_only(crud_store, workspace_id, thread_id, turn_id).await;
        persist_test_execution_security_snapshot(crud_store, thread_id, turn_id).await;
    }

    async fn persist_test_runtime_snapshot_only(
        crud_store: &CrudStore,
        workspace_id: &str,
        thread_id: &str,
        turn_id: &str,
    ) {
        let mut workspace_skill_policies = HashMap::new();
        let writer_skill_id =
            pioneer_protocol::SkillId::new("WWWWWWWWWWWWWWWWWWWWW").expect("valid writer SkillId");
        workspace_skill_policies.insert(
            SkillPolicyKey::new(writer_skill_id.clone()),
            WorkspaceSkillPolicy {
                enabled: Some(true),
                allow_implicit_invocation: Some(false),
            },
        );
        let capabilities = vec![TurnCapability {
            id: format!("skill:{writer_skill_id}"),
            kind: TurnCapabilityKind::Skill {
                skill_id: writer_skill_id,
                pack_id: None,
            },
            label: Some("Writer".to_owned()),
        }];
        let resolved_artifacts = vec![ResolvedArtifactInput {
            artifact_id: "artifact_snapshot".to_owned(),
            version_id: Some("version_snapshot".to_owned()),
            content_type: InputContentType::File,
            attachment: MessageAttachment::from_path("/tmp/snapshot.txt", "text/plain"),
        }];
        let runtime_environment = HashMap::from([(
            "PIONEER_ARTIFACT_OUTPUT_DIR".to_owned(),
            "/tmp/pioneer-snapshot-output".to_owned(),
        )]);
        let history = vec![
            ChatMessage::user("previous user message"),
            ChatMessage::assistant("previous assistant message"),
        ];
        let input = vec![UserInput::Text {
            text: "run tool".to_owned(),
            text_elements: Vec::new(),
        }];
        let hook_runtime_context = AgentTurnHookRuntimeContext::task("task_snapshot");

        let snapshot = crate::turn_runtime_snapshot::new_turn_runtime_snapshot(
            thread_id,
            workspace_id,
            turn_id,
            ThreadMode::Agent,
            &hook_runtime_context,
            "test-model",
            "echo",
            None,
            &workspace_skill_policies,
            input.as_slice(),
            capabilities.as_slice(),
            resolved_artifacts.as_slice(),
            &runtime_environment,
            history.as_slice(),
            &[],
        )
        .expect("runtime snapshot should serialize");
        crud_store
            .upsert_turn_runtime_snapshot(snapshot)
            .await
            .expect("runtime snapshot should persist");
    }

    async fn persist_test_execution_security_snapshot(
        crud_store: &CrudStore,
        thread_id: &str,
        turn_id: &str,
    ) {
        let (_, turn) = crud_store
            .get_turn(thread_id, turn_id)
            .await
            .expect("turn should load for security snapshot")
            .expect("turn should exist for security snapshot");
        let snapshot = test_execution_security_snapshot(turn.permission_profile);
        crud_store
            .set_turn_execution_security_snapshot(turn_id, &snapshot)
            .await
            .expect("security snapshot should persist");
    }

    fn test_execution_security_snapshot(
        permission_profile: TurnPermissionProfileSnapshot,
    ) -> TurnExecutionSecuritySnapshot {
        let workspace_root = "/tmp/pioneer-recovery-security";
        TurnExecutionSecuritySnapshot::workspace_write(
            permission_profile,
            workspace_root,
            vec![TurnFilesystemSandboxEntry::workspace_root(
                TurnFilesystemAccess::Write,
                workspace_root,
            )],
            1_700_000_000_000,
        )
    }

    #[tokio::test]
    async fn restored_recovery_turn_request_uses_runtime_snapshot() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_restore_snapshot";
        let thread_id = "thr_restore_snapshot";
        let turn_id = "turn_restore_snapshot";
        let permission_profile = TurnPermissionProfileSnapshot::from_mode(
            TurnPermissionMode::Supervised,
            TurnPermissionProfileSource::Composer,
        );
        materialize_turn_with_tool_item_and_permission_profile(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_restore_snapshot",
            permission_profile.clone(),
            None,
        )
        .await;
        let skill_dir = std::env::temp_dir().join(format!("pioneer-recovery-skill-{turn_id}"));
        std::fs::create_dir_all(&skill_dir).expect("recovery skill directory should exist");
        std::fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: Writer\nslug: writer\ndescription: Recovery writer skill\n---\nWrite carefully.",
        )
        .expect("recovery skill package should exist");
        let writer_skill_id =
            pioneer_protocol::SkillId::new("WWWWWWWWWWWWWWWWWWWWW").expect("valid writer SkillId");
        let pack_id =
            pioneer_protocol::SkillPackId::new("PPPPPPPPPPPPPPPPPPPPP").expect("valid pack id");
        crud_store
            .insert_skill_pack_installation_with_children(
                &SkillPackInstallationRecord {
                    pack_id: pack_id.clone(),
                    name: "Recovery Pack".to_owned(),
                    scope_key: workspace_id.to_owned(),
                    source_kind: "user".to_owned(),
                    created_at_unix: 1,
                    updated_at_unix: 1,
                },
                &[SkillInstallationRecord {
                    skill_id: writer_skill_id.clone(),
                    owner: Some("tests".to_owned()),
                    slug: "writer".to_owned(),
                    version: None,
                    source_kind: "user".to_owned(),
                    scope_key: workspace_id.to_owned(),
                    source_ref: "test:recovery:writer".to_owned(),
                    install_path: skill_dir.display().to_string(),
                    trust_level: "verified".to_owned(),
                    fingerprint: "recovery-writer-fingerprint".to_owned(),
                    updated_at_unix: 1,
                    pack_id: Some(pack_id.clone()),
                    pack_member_key: Some("writer".to_owned()),
                }],
            )
            .await
            .expect("recovery skill installation should persist");
        persist_test_runtime_snapshot(crud_store.as_ref(), workspace_id, thread_id, turn_id).await;
        let stored_snapshot = crud_store
            .get_turn_runtime_snapshot(turn_id)
            .await
            .expect("runtime snapshot should query")
            .expect("runtime snapshot should exist");
        assert!(!stored_snapshot.capabilities_json.contains("packId"));
        assert!(!stored_snapshot.capabilities_json.contains("skillPack"));

        assert!(
            crud_store
                .update_skill_pack_installation_name(
                    workspace_id,
                    &pack_id,
                    "Renamed Recovery Pack",
                    2,
                )
                .await
                .expect("pack rename should succeed")
        );
        std::fs::write(
            skill_dir.join("SKILL.md"),
            "---\nname: Writer\nslug: writer\ndescription: Updated recovery writer skill\n---\nWrite with current content.",
        )
        .expect("updated recovery skill package should exist");
        assert!(
            crud_store
                .delete_skill_pack_installation_with_children(workspace_id, &pack_id)
                .await
                .expect("pack removal should succeed")
        );
        crud_store
            .insert_skill_installation(
                &SkillInstallationRecord {
                    skill_id: writer_skill_id.clone(),
                    owner: Some("tests".to_owned()),
                    slug: "writer".to_owned(),
                    version: None,
                    source_kind: "user".to_owned(),
                    scope_key: workspace_id.to_owned(),
                    source_ref: "test:recovery:writer-current".to_owned(),
                    install_path: skill_dir.display().to_string(),
                    trust_level: "verified".to_owned(),
                    fingerprint: "recovery-writer-current-fingerprint".to_owned(),
                    updated_at_unix: 3,
                    pack_id: None,
                    pack_member_key: None,
                },
                3,
            )
            .await
            .expect("retained SkillId should remain available independently of deleted parent");
        assert!(
            crud_store
                .find_skill_pack_installation(workspace_id, &pack_id)
                .await
                .expect("deleted parent lookup")
                .is_none()
        );

        let restored = coordinator
            .restored_recovery_turn_request(thread_id, turn_id, 1_700_000_000)
            .await
            .expect("restored request should load")
            .into_available()
            .expect("runtime snapshot should be restorable");

        assert_eq!(restored.turn_id, turn_id);
        assert_eq!(restored.execution_window_index, 2);
        assert_eq!(restored.provider_name, "echo");
        assert_eq!(
            restored.hook_runtime_context.task_id.as_deref(),
            Some("task_snapshot")
        );
        assert_eq!(restored.workspace_skill_policies.len(), 1);
        assert_eq!(restored.capabilities.len(), 1);
        let restored_skill_id = writer_skill_id;
        assert!(
            restored
                .workspace_skill_policies
                .contains_key(&SkillPolicyKey::new(restored_skill_id.clone()))
        );
        assert!(matches!(
            &restored.capabilities[0].kind,
            TurnCapabilityKind::Skill { skill_id, .. } if skill_id == &restored_skill_id
        ));
        assert_eq!(
            restored.capabilities[0].id,
            format!("skill:{restored_skill_id}")
        );
        let restored_skill = restored
            .skill_catalog
            .skills
            .iter()
            .find(|skill| skill.identity.skill_id == restored_skill_id)
            .expect("frozen SkillId should resolve without its former parent");
        assert_eq!(restored_skill.identity.owner.as_deref(), Some("tests"));
        assert_eq!(restored_skill.identity.slug, "writer");
        assert_eq!(
            restored_skill.instructions.body.trim(),
            "Write with current content."
        );
        assert_eq!(restored.resolved_artifacts.len(), 1);
        assert_eq!(
            restored
                .runtime_environment
                .get("PIONEER_ARTIFACT_OUTPUT_DIR")
                .map(String::as_str),
            Some("/tmp/pioneer-snapshot-output")
        );
        assert_eq!(restored.history.len(), 2);
        assert_eq!(restored.permission_profile, permission_profile);
        let security_snapshot = restored
            .execution_security_snapshot
            .expect("restored request should carry persisted security snapshot");
        assert_eq!(security_snapshot.permission_profile, permission_profile);
        assert_eq!(
            security_snapshot.sandbox.mode,
            TurnSandboxMode::WorkspaceWrite
        );
        assert!(
            crud_store
                .delete_skill_installation(&restored_skill_id)
                .await
                .expect("selected skill removal should succeed")
        );
        let restored_missing = coordinator
            .restored_recovery_turn_request(thread_id, turn_id, 1_700_000_001)
            .await
            .expect("missing-skill recovery request should load")
            .into_available()
            .expect("runtime snapshot remains the authoritative identity set");
        assert!(matches!(
            &restored_missing.capabilities[0].kind,
            TurnCapabilityKind::Skill { skill_id, .. } if skill_id == &restored_skill_id
        ));
        assert!(
            restored_missing
                .skill_catalog
                .skills
                .iter()
                .all(|skill| skill.identity.skill_id != restored_skill_id),
            "removed frozen child must remain missing instead of being replaced"
        );
        let _ = std::fs::remove_dir_all(skill_dir);
    }

    #[tokio::test]
    async fn cli_runtime_recovery_routes_through_native_attempt_without_runtime_snapshot() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_cli_native_recovery";
        let thread_id = "thr_cli_native_recovery";
        let turn_id = "turn_cli_native_recovery";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_cli_native_recovery",
            None,
        )
        .await;
        let timestamp = chrono::Utc::now().fixed_offset();
        crud_store
            .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
                turn_id: turn_id.to_owned(),
                thread_id: thread_id.to_owned(),
                continuation_thread_id: thread_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "native-thread-cli-recovery".to_owned(),
                native_turn_id: Some("native-turn-cli-recovery-1".to_owned()),
                request_id: None,
                status: crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING
                    .to_owned(),
                model: Some("gpt-5".to_owned()),
                cwd: Some("/tmp/project".to_owned()),
                sandbox_json: None,
                approval_policy: Some("on-request".to_owned()),
                input_mapping_json: "{}".to_owned(),
                created_at: timestamp,
                updated_at: timestamp,
            })
            .await
            .expect("CLI runtime binding should persist");
        let first_window = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 1,
                    tool_call_count: 1,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "cli_window_1"}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("initial CLI execution window should persist");
        assert!(
            crud_store
                .get_turn_runtime_snapshot(turn_id)
                .await
                .expect("runtime snapshot lookup should succeed")
                .is_none(),
            "CLI-backed turns must not require an API-agent runtime snapshot"
        );
        let job = coordinator
            .enqueue_runtime_failure_job(
                &RuntimeFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "runtime:runtime_failure".to_owned(),
                    item_type: TurnItemType::SystemEvent,
                    trigger: RecoveryTrigger::RuntimeFailure,
                    action: RecoveryAction::RestartTurn,
                    reason: "native CLI turn interrupted".to_owned(),
                    base_backoff_secs: 0,
                    max_attempts: 3,
                    max_wall_clock_secs: 180,
                    no_progress_limit: 3,
                    metadata: pioneer_protocol::ToolMetadata::default(),
                },
                1_700_000_010,
            )
            .await
            .expect("CLI runtime recovery job should enqueue")
            .into_job();

        let events = coordinator
            .run_ready_jobs(1_700_000_011, 1)
            .await
            .expect("CLI runtime recovery job should run");
        let [RecoveryCoordinatorEvent::CliRuntimeRetryAttemptRequested(request)] =
            events.as_slice()
        else {
            panic!("expected one CLI runtime recovery request, got {events:?}");
        };
        assert_eq!(request.job_id, job.id);
        assert_eq!(request.turn_id, turn_id);
        assert_eq!(request.attempt_number, 1);
        assert_eq!(request.execution_window_index, 2);
        assert_eq!(
            request.binding.native_thread_id,
            "native-thread-cli-recovery"
        );
        let active_job = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .expect("active recovery job should load")
            .expect("active recovery job should exist");
        assert_eq!(active_job.status, RecoveryJobStatus::Active);
        assert_eq!(
            active_job.active_attempt_id.as_deref(),
            Some(request.recovery_attempt_id.as_str())
        );
        let checkpointed = crud_store
            .get_turn_execution_window(first_window.id.as_str())
            .await
            .expect("first execution window should load")
            .expect("first execution window should exist");
        assert_eq!(checkpointed.status, ExecutionWindowStatus::Checkpointed);
        assert_eq!(
            checkpointed
                .metadata_json
                .get("interruptedBy")
                .and_then(serde_json::Value::as_str),
            Some("cli_runtime_recovery")
        );
        assert_eq!(
            crud_store
                .list_turn_execution_checkpoints_for_window(first_window.id.as_str())
                .await
                .expect("CLI recovery checkpoint should load")
                .len(),
            1
        );

        let failure_events = coordinator
            .record_cli_runtime_attempt_failure(
                request.job_id.as_str(),
                request.recovery_attempt_id.as_str(),
                "replacement native turn disconnected".to_owned(),
                1_700_000_012,
            )
            .await
            .expect("native CLI recovery failure should return to coordinator policy");
        assert!(matches!(
            failure_events.as_slice(),
            [RecoveryCoordinatorEvent::RetryScheduled { .. }]
        ));
        let pending_job = crud_store
            .get_recovery_job(request.job_id.as_str())
            .await
            .expect("retrying recovery job should load")
            .expect("retrying recovery job should exist");
        assert_eq!(pending_job.status, RecoveryJobStatus::Pending);
        assert_eq!(pending_job.run_count, 1);
    }

    #[tokio::test]
    async fn legacy_cli_recovery_backfills_window_one_before_opening_window_two() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_cli_legacy_window";
        let thread_id = "thr_cli_legacy_window";
        let turn_id = "turn_cli_legacy_window";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_cli_legacy_window",
            None,
        )
        .await;
        let timestamp = chrono::Utc::now().fixed_offset();
        crud_store
            .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
                turn_id: turn_id.to_owned(),
                thread_id: thread_id.to_owned(),
                continuation_thread_id: thread_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "native-thread-cli-legacy".to_owned(),
                native_turn_id: Some("native-turn-cli-legacy".to_owned()),
                request_id: None,
                status: crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING
                    .to_owned(),
                model: Some("gpt-5".to_owned()),
                cwd: Some("/tmp/project".to_owned()),
                sandbox_json: None,
                approval_policy: Some("on-request".to_owned()),
                input_mapping_json: "{}".to_owned(),
                created_at: timestamp,
                updated_at: timestamp,
            })
            .await
            .expect("legacy CLI binding should persist");

        let admission = coordinator
            .prepare_recovery_execution_window(
                turn_id,
                1_700_000_010,
                "cli_runtime_recovery",
                "legacy native CLI turn was replaced",
            )
            .await
            .expect("legacy CLI recovery window should be prepared");

        assert_eq!(
            admission,
            super::ExecutionWindowContinuationAdmission::Open { window_index: 2 }
        );
        let windows = crud_store
            .list_turn_execution_windows(turn_id)
            .await
            .expect("backfilled execution window should load");
        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].window_index, 1);
        assert_eq!(windows[0].status, ExecutionWindowStatus::Checkpointed);
        assert_eq!(
            windows[0]
                .metadata_json
                .get("recoveredFromMissingWindow")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            crud_store
                .list_turn_execution_checkpoints_for_window(windows[0].id.as_str())
                .await
                .expect("backfilled CLI recovery checkpoint should load")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn cli_recovery_checkpoint_counts_progress_only_in_the_current_window() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_cli_current_window_progress";
        let thread_id = "thr_cli_current_window_progress";
        let turn_id = "turn_cli_current_window_progress";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_before_current_window",
            None,
        )
        .await;
        let timestamp = chrono::Utc::now().fixed_offset();
        crud_store
            .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
                turn_id: turn_id.to_owned(),
                thread_id: thread_id.to_owned(),
                continuation_thread_id: thread_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
                runtime_id: "claude".to_owned(),
                runtime_kind: "claude".to_owned(),
                native_thread_id: "native-thread-current-window".to_owned(),
                native_turn_id: Some("native-turn-current-window".to_owned()),
                request_id: None,
                status: crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING
                    .to_owned(),
                model: Some("claude-sonnet".to_owned()),
                cwd: Some("/tmp/project".to_owned()),
                sandbox_json: None,
                approval_policy: Some("on-request".to_owned()),
                input_mapping_json: "{}".to_owned(),
                created_at: timestamp,
                updated_at: timestamp,
            })
            .await
            .expect("CLI runtime binding should persist");
        crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Checkpointed,
                    exhaustion_reason: Some(
                        ExecutionWindowExhaustionReason::MaxWallClockMsPerWindow,
                    ),
                    agent_round_count: 5,
                    tool_call_count: 7,
                    provider_token_count: 100,
                    metadata_json: serde_json::json!({}),
                    started_at: timestamp - chrono::Duration::hours(1),
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("prior progress window should persist");
        let current = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 2,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({}),
                    started_at: timestamp + chrono::Duration::seconds(1),
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("current empty window should persist");

        assert_eq!(
            coordinator
                .prepare_recovery_execution_window(
                    turn_id,
                    timestamp.timestamp().saturating_add(2),
                    "cli_runtime_recovery",
                    "runtime exited before producing progress",
                )
                .await
                .expect("current CLI window should checkpoint"),
            super::ExecutionWindowContinuationAdmission::Open { window_index: 3 }
        );

        let current = crud_store
            .get_turn_execution_window(current.id.as_str())
            .await
            .expect("current window should load")
            .expect("current window should exist");
        assert_eq!(current.status, ExecutionWindowStatus::Checkpointed);
        assert_eq!(current.agent_round_count, 0);
        assert_eq!(current.tool_call_count, 0);
        let usage = crud_store
            .aggregate_turn_execution_window_usage(turn_id)
            .await
            .expect("window usage should aggregate");
        assert_eq!(usage.total_agent_rounds, 5);
        assert_eq!(usage.total_tool_calls, 7);
        assert_eq!(usage.consecutive_no_progress_windows, 1);
    }

    #[tokio::test]
    async fn virtual_week_with_causal_progress_survives_windows_and_coordinator_restart() {
        let (crud_store, agent_manager, coordinator) = setup_coordinator_with_agent().await;
        let timestamp = chrono::DateTime::from_timestamp(1_700_000_000, 0)
            .expect("fixed virtual-week timestamp should be valid")
            .fixed_offset();
        let turn_id = "turn_virtual_week_progress";
        let virtual_week_windows = 7 * 24;
        for window_index in 1..=virtual_week_windows {
            let window_started_at = timestamp + chrono::Duration::hours(i64::from(window_index));
            crud_store
                .create_turn_execution_window(
                    NewTurnExecutionWindowRecord {
                        workspace_id: "ws_virtual_week_progress".to_owned(),
                        thread_id: "thr_virtual_week_progress".to_owned(),
                        turn_id: turn_id.to_owned(),
                        window_index,
                        status: ExecutionWindowStatus::Checkpointed,
                        exhaustion_reason: Some(
                            ExecutionWindowExhaustionReason::MaxWallClockMsPerWindow,
                        ),
                        agent_round_count: 1,
                        tool_call_count: 1,
                        provider_token_count: 10,
                        metadata_json: serde_json::json!({
                            "runtimeWindowId": format!("{turn_id}:window:{window_index}"),
                        }),
                        started_at: window_started_at,
                    },
                    window_started_at,
                    window_started_at,
                )
                .await
                .expect("healthy execution window should persist");
        }

        drop(coordinator);
        let restarted = RecoveryCoordinator::new(
            crud_store.clone(),
            agent_manager,
            Arc::new(ProviderRegistry::with_provider(
                "echo",
                Arc::new(EchoProvider::new()),
            )),
            RecoveryPolicyRegistry::default(),
            test_tool_loop_config().normalized(),
        );

        assert_eq!(
            restarted
                .execution_window_continuation_admission_for_turn(turn_id, None)
                .await
                .expect("a virtual week of durable causal progress should survive restart"),
            super::ExecutionWindowContinuationAdmission::Open {
                window_index: virtual_week_windows + 1,
            }
        );
    }

    #[tokio::test]
    async fn persisted_no_progress_windows_trip_recovery_circuit_breaker_after_coordinator_restart()
    {
        let (crud_store, agent_manager, coordinator) = setup_coordinator_with_agent().await;
        let timestamp = chrono::Utc::now().fixed_offset();
        let turn_id = "turn_recovery_no_progress";
        for window_index in 1..=3 {
            crud_store
                .create_turn_execution_window(
                    NewTurnExecutionWindowRecord {
                        workspace_id: "ws_recovery_no_progress".to_owned(),
                        thread_id: "thr_recovery_no_progress".to_owned(),
                        turn_id: turn_id.to_owned(),
                        window_index,
                        status: ExecutionWindowStatus::Interrupted,
                        exhaustion_reason: Some(
                            ExecutionWindowExhaustionReason::RuntimeShutdownContinuation,
                        ),
                        agent_round_count: 0,
                        tool_call_count: 0,
                        provider_token_count: 0,
                        metadata_json: serde_json::json!({
                            "interruptedBy": "cli_runtime_recovery",
                            "terminalReason": "native runtime exited before producing output",
                        }),
                        started_at: timestamp,
                    },
                    timestamp,
                    timestamp,
                )
                .await
                .expect("no-progress execution window should persist");
        }

        drop(coordinator);
        let restarted = RecoveryCoordinator::new(
            crud_store.clone(),
            agent_manager,
            Arc::new(ProviderRegistry::with_provider(
                "echo",
                Arc::new(EchoProvider::new()),
            )),
            RecoveryPolicyRegistry::default(),
            test_tool_loop_config().normalized(),
        );

        let admission = restarted
            .execution_window_continuation_admission_for_turn(turn_id, None)
            .await
            .expect("no-progress admission should be evaluated");
        assert!(matches!(
            admission,
            super::ExecutionWindowContinuationAdmission::Block {
                total_windows: 3,
                ref reason,
            } if reason.contains("max_consecutive_no_progress_windows")
        ));
    }

    #[tokio::test]
    async fn durable_progress_resets_persisted_recovery_circuit_breaker() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let timestamp = chrono::Utc::now().fixed_offset();
        let turn_id = "turn_recovery_progress_reset";
        for window_index in 1..=2 {
            crud_store
                .create_turn_execution_window(
                    NewTurnExecutionWindowRecord {
                        workspace_id: "ws_recovery_progress_reset".to_owned(),
                        thread_id: "thr_recovery_progress_reset".to_owned(),
                        turn_id: turn_id.to_owned(),
                        window_index,
                        status: ExecutionWindowStatus::Interrupted,
                        exhaustion_reason: Some(
                            ExecutionWindowExhaustionReason::ProviderFailureContinuation,
                        ),
                        agent_round_count: 0,
                        tool_call_count: 0,
                        provider_token_count: 0,
                        metadata_json: serde_json::json!({}),
                        started_at: timestamp,
                    },
                    timestamp,
                    timestamp,
                )
                .await
                .expect("no-progress execution window should persist");
        }
        crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: "ws_recovery_progress_reset".to_owned(),
                    thread_id: "thr_recovery_progress_reset".to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 3,
                    status: ExecutionWindowStatus::Interrupted,
                    exhaustion_reason: Some(
                        ExecutionWindowExhaustionReason::RuntimeShutdownContinuation,
                    ),
                    agent_round_count: 1,
                    tool_call_count: 0,
                    provider_token_count: 10,
                    metadata_json: serde_json::json!({}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("progress execution window should persist");

        assert_eq!(
            coordinator
                .execution_window_continuation_admission_for_turn(turn_id, None)
                .await
                .expect("progress should reset the recovery circuit breaker"),
            super::ExecutionWindowContinuationAdmission::Open { window_index: 4 }
        );
    }

    #[tokio::test]
    async fn cli_runtime_recovery_blocks_before_exceeding_the_shared_window_limit() {
        let mut tool_loop_config = test_tool_loop_config();
        tool_loop_config
            .execution_windows
            .total
            .max_windows_per_turn = Some(1);
        let (crud_store, _agent_manager, coordinator) =
            setup_coordinator_with_tool_loop_config(tool_loop_config).await;
        let workspace_id = "ws_cli_window_limit";
        let thread_id = "thr_cli_window_limit";
        let turn_id = "turn_cli_window_limit";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_cli_window_limit",
            None,
        )
        .await;
        let timestamp = chrono::Utc::now().fixed_offset();
        crud_store
            .upsert_cli_runtime_turn_binding(NewCliRuntimeTurnBinding {
                turn_id: turn_id.to_owned(),
                thread_id: thread_id.to_owned(),
                continuation_thread_id: thread_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
                runtime_id: "codex".to_owned(),
                runtime_kind: "codex".to_owned(),
                native_thread_id: "native-thread-cli-window-limit".to_owned(),
                native_turn_id: Some("native-turn-cli-window-limit".to_owned()),
                request_id: None,
                status: crate::cli_runtime::turn_binding::CLI_RUNTIME_TURN_STATUS_RUNNING
                    .to_owned(),
                model: Some("gpt-5".to_owned()),
                cwd: Some("/tmp/project".to_owned()),
                sandbox_json: None,
                approval_policy: Some("on-request".to_owned()),
                input_mapping_json: "{}".to_owned(),
                created_at: timestamp,
                updated_at: timestamp,
            })
            .await
            .expect("CLI runtime binding should persist");
        let first_window = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 1,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "cli_window_1"}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("initial CLI execution window should persist");
        let job = coordinator
            .enqueue_runtime_failure_job(
                &RuntimeFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "runtime:cli_window_limit".to_owned(),
                    item_type: TurnItemType::SystemEvent,
                    trigger: RecoveryTrigger::RuntimeFailure,
                    action: RecoveryAction::RestartTurn,
                    reason: "native CLI turn interrupted".to_owned(),
                    base_backoff_secs: 0,
                    max_attempts: 3,
                    max_wall_clock_secs: 180,
                    no_progress_limit: 3,
                    metadata: pioneer_protocol::ToolMetadata::default(),
                },
                1_700_000_010,
            )
            .await
            .expect("CLI runtime recovery job should enqueue")
            .into_job();

        let events = coordinator
            .run_ready_jobs(1_700_000_011, 1)
            .await
            .expect("CLI runtime recovery limit should be evaluated");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoveryBlocked {
                job_id,
                turn_id: event_turn_id,
                reason,
            }] if job_id == &job.id
                && event_turn_id == turn_id
                && reason.contains("limit=1, observed=1")
        ));
        let blocked_job = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .expect("blocked recovery job should load")
            .expect("blocked recovery job should exist");
        assert_eq!(blocked_job.status, RecoveryJobStatus::Blocked);
        let checkpointed_window = crud_store
            .get_turn_execution_window(first_window.id.as_str())
            .await
            .expect("first window should load")
            .expect("first window should exist");
        assert_eq!(
            checkpointed_window.status,
            ExecutionWindowStatus::Checkpointed
        );
        assert_eq!(
            crud_store
                .list_turn_execution_windows(turn_id)
                .await
                .expect("execution windows should load")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn native_recovery_blocks_before_exceeding_the_shared_window_limit() {
        let mut tool_loop_config = test_tool_loop_config();
        tool_loop_config
            .execution_windows
            .total
            .max_windows_per_turn = Some(1);
        let (crud_store, _agent_manager, coordinator) =
            setup_coordinator_with_tool_loop_config(tool_loop_config).await;
        let workspace_id = "ws_native_window_limit";
        let thread_id = "thr_native_window_limit";
        let turn_id = "turn_native_window_limit";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_native_window_limit",
            None,
        )
        .await;
        persist_test_runtime_snapshot(crud_store.as_ref(), workspace_id, thread_id, turn_id).await;
        let timestamp = chrono::Utc::now().fixed_offset();
        let first_window = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 1,
                    tool_call_count: 1,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "native_window_1"}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("initial native execution window should persist");
        let job = coordinator
            .enqueue_runtime_failure_job(
                &RuntimeFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "runtime:native_window_limit".to_owned(),
                    item_type: TurnItemType::SystemEvent,
                    trigger: RecoveryTrigger::RuntimeFailure,
                    action: RecoveryAction::RestartTurn,
                    reason: "native API turn interrupted".to_owned(),
                    base_backoff_secs: 0,
                    max_attempts: 3,
                    max_wall_clock_secs: 180,
                    no_progress_limit: 3,
                    metadata: pioneer_protocol::ToolMetadata::default(),
                },
                1_700_000_010,
            )
            .await
            .expect("native recovery job should enqueue")
            .into_job();

        let events = coordinator
            .run_ready_jobs(1_700_000_011, 1)
            .await
            .expect("native recovery limit should be evaluated");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoveryBlocked {
                job_id,
                turn_id: event_turn_id,
                reason,
            }] if job_id == &job.id
                && event_turn_id == turn_id
                && reason.contains("limit=1, observed=1")
        ));
        let checkpointed_window = crud_store
            .get_turn_execution_window(first_window.id.as_str())
            .await
            .expect("first window should load")
            .expect("first window should exist");
        assert_eq!(
            checkpointed_window.status,
            ExecutionWindowStatus::Checkpointed
        );
        assert_eq!(
            crud_store
                .list_turn_execution_windows(turn_id)
                .await
                .expect("execution windows should load")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn restored_recovery_turn_request_checkpoints_stale_window_and_advances_index() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_restore_running_window";
        let thread_id = "thr_restore_running_window";
        let turn_id = "turn_restore_running_window";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_restore_running_window",
            None,
        )
        .await;
        persist_test_runtime_snapshot(crud_store.as_ref(), workspace_id, thread_id, turn_id).await;

        let timestamp = chrono::Utc::now().fixed_offset();
        let window = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 1,
                    tool_call_count: 1,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "stale_window_1"}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("running window should persist");

        let restored = coordinator
            .restored_recovery_turn_request(thread_id, turn_id, 1_700_000_010)
            .await
            .expect("restored request should load")
            .into_available()
            .expect("runtime snapshot should be restorable");
        assert_eq!(restored.execution_window_index, 2);

        let checkpointed = crud_store
            .get_turn_execution_window(window.id.as_str())
            .await
            .expect("window read should succeed")
            .expect("window should exist");
        assert_eq!(checkpointed.status, ExecutionWindowStatus::Checkpointed);
        assert_eq!(
            checkpointed.exhaustion_reason,
            Some(ExecutionWindowExhaustionReason::RuntimeShutdownContinuation)
        );
        assert_eq!(
            checkpointed
                .metadata_json
                .get("interruptedBy")
                .and_then(serde_json::Value::as_str),
            Some("startup_recovery")
        );
        let checkpoints = crud_store
            .list_turn_execution_checkpoints_for_window(window.id.as_str())
            .await
            .expect("startup checkpoint should load");
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            checkpoints[0].checkpoint_kind,
            TurnExecutionCheckpointKind::StartupRecovery
        );
        let payload: pioneer_protocol::ExecutionCheckpointPayload =
            serde_json::from_value(checkpoints[0].payload_json.clone())
                .expect("startup checkpoint payload should decode");
        assert_eq!(
            payload.window.exhaustion_reason,
            Some(ExecutionWindowExhaustionReason::RuntimeShutdownContinuation)
        );
        assert_eq!(payload.provider_budget.exhausted_limit, None);
        assert_eq!(payload.provider_budget.exhausted_observed, None);
    }

    #[tokio::test]
    async fn startup_recovery_refuses_to_create_a_window_past_the_shared_limit() {
        let mut tool_loop_config = test_tool_loop_config();
        tool_loop_config
            .execution_windows
            .total
            .max_windows_per_turn = Some(1);
        let (crud_store, _agent_manager, coordinator) =
            setup_coordinator_with_tool_loop_config(tool_loop_config).await;
        let workspace_id = "ws_startup_window_limit";
        let thread_id = "thr_startup_window_limit";
        let turn_id = "turn_startup_window_limit";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_startup_window_limit",
            None,
        )
        .await;
        persist_test_runtime_snapshot(crud_store.as_ref(), workspace_id, thread_id, turn_id).await;
        let timestamp = chrono::Utc::now().fixed_offset();
        crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 1,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "startup_window_1"}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("initial startup execution window should persist");

        let restored = coordinator
            .restored_recovery_turn_request(thread_id, turn_id, 1_700_000_010)
            .await
            .expect("startup recovery lookup should succeed");

        assert!(matches!(
            restored,
            super::RestoredRecoveryTurnRequestLookup::Unavailable(
                super::RestoredRecoveryTurnUnavailable::ExecutionWindowContinuationBlocked {
                    ref reason,
                }
            ) if reason.contains("limit=1, observed=1")
        ));
        assert_eq!(
            crud_store
                .list_turn_execution_windows(turn_id)
                .await
                .expect("execution windows should load")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn runtime_recovery_checkpoints_running_window_before_restart() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_runtime_checkpoint";
        let thread_id = "thr_runtime_checkpoint";
        let turn_id = "turn_runtime_checkpoint";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_runtime_checkpoint",
            None,
        )
        .await;
        persist_test_runtime_snapshot(crud_store.as_ref(), workspace_id, thread_id, turn_id).await;

        let timestamp = chrono::Utc::now().fixed_offset();
        let window = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "runtime_window_1"}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("running window should persist");
        let job = coordinator
            .enqueue_runtime_failure_job(
                &RuntimeFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "runtime:panic".to_owned(),
                    item_type: TurnItemType::SystemEvent,
                    trigger: RecoveryTrigger::RuntimeFailure,
                    action: RecoveryAction::RestartTurn,
                    reason: "agent turn task panicked".to_owned(),
                    base_backoff_secs: 0,
                    max_attempts: 3,
                    max_wall_clock_secs: 180,
                    no_progress_limit: 3,
                    metadata: pioneer_protocol::ToolMetadata::default(),
                },
                timestamp.timestamp(),
            )
            .await
            .expect("runtime recovery job should enqueue")
            .into_job();

        let context = coordinator
            .prepare_execution_checkpoint_for_recovery(
                workspace_id,
                thread_id,
                &job,
                timestamp.timestamp().saturating_add(1),
            )
            .await
            .expect("runtime checkpoint preparation should succeed")
            .expect("runtime recovery should receive a checkpoint");
        assert_eq!(context.window_id, "runtime_window_1");
        assert_eq!(context.window_index, 1);
        assert_eq!(context.next_window_index(), 2);
        assert_eq!(context.checkpoint_kind, "window_exhausted");
        assert_eq!(
            context.payload.window.exhaustion_reason,
            Some(ExecutionWindowExhaustionReason::RuntimeShutdownContinuation)
        );

        let checkpointed = crud_store
            .get_turn_execution_window(window.id.as_str())
            .await
            .expect("window read should succeed")
            .expect("window should exist");
        assert_eq!(checkpointed.status, ExecutionWindowStatus::Checkpointed);
        assert_eq!(
            checkpointed
                .metadata_json
                .get("interruptedBy")
                .and_then(serde_json::Value::as_str),
            Some("runtime_failure_recovery")
        );
        let checkpoints = crud_store
            .list_turn_execution_checkpoints_for_window(window.id.as_str())
            .await
            .expect("runtime checkpoint should load");
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(
            checkpoints[0].checkpoint_kind,
            TurnExecutionCheckpointKind::WindowExhausted
        );

        let second_timestamp = timestamp + chrono::Duration::seconds(2);
        let second_window = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 2,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "runtime_window_2"}),
                    started_at: second_timestamp,
                },
                second_timestamp,
                second_timestamp,
            )
            .await
            .expect("second running window should persist");
        let second_context = coordinator
            .prepare_execution_checkpoint_for_recovery(
                workspace_id,
                thread_id,
                &job,
                second_timestamp.timestamp().saturating_add(1),
            )
            .await
            .expect("second runtime checkpoint preparation should succeed")
            .expect("second runtime recovery should receive the current checkpoint");
        assert_eq!(second_context.window_id, "runtime_window_2");
        assert_eq!(second_context.window_index, 2);
        assert_eq!(second_context.next_window_index(), 3);
        assert_eq!(
            crud_store
                .get_turn_execution_window(second_window.id.as_str())
                .await
                .expect("second window read should succeed")
                .expect("second window should exist")
                .status,
            ExecutionWindowStatus::Checkpointed
        );
    }

    #[tokio::test]
    async fn restored_recovery_turn_request_does_not_close_stale_window_for_invalid_snapshot() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_restore_invalid_snapshot";
        let thread_id = "thr_restore_invalid_snapshot";
        let turn_id = "turn_restore_invalid_snapshot";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_restore_invalid_snapshot",
            None,
        )
        .await;

        let timestamp = chrono::Utc::now().fixed_offset();
        crud_store
            .upsert_turn_runtime_snapshot(NewTurnRuntimeSnapshot {
                turn_id: turn_id.to_owned(),
                thread_id: thread_id.to_owned(),
                workspace_id: workspace_id.to_owned(),
                mode_json: "{invalid".to_owned(),
                model: "test-model".to_owned(),
                provider_name: "echo".to_owned(),
                reasoning_effort: None,
                agent_skill_versions_json: None,
                hook_runtime_context_json: "{}".to_owned(),
                workspace_skill_policies_json: "[]".to_owned(),
                input_json: "[]".to_owned(),
                capabilities_json: "[]".to_owned(),
                resolved_artifacts_json: "[]".to_owned(),
                runtime_environment_json: "{}".to_owned(),
                history_json: "[]".to_owned(),
                created_at: timestamp,
                updated_at: timestamp,
            })
            .await
            .expect("invalid runtime snapshot should persist");
        persist_test_execution_security_snapshot(crud_store.as_ref(), thread_id, turn_id).await;
        let window = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 1,
                    tool_call_count: 1,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "stale_window_1"}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("running window should persist");

        let lookup = coordinator
            .restored_recovery_turn_request(thread_id, turn_id, 1_700_000_010)
            .await
            .expect("restored request lookup should evaluate");
        assert!(matches!(
            lookup,
            super::RestoredRecoveryTurnRequestLookup::Unavailable(
                super::RestoredRecoveryTurnUnavailable::SnapshotInvalid { .. }
            )
        ));

        let still_running = crud_store
            .get_turn_execution_window(window.id.as_str())
            .await
            .expect("window read should succeed")
            .expect("window should exist");
        assert_eq!(still_running.status, ExecutionWindowStatus::Running);
    }

    #[tokio::test]
    async fn restored_recovery_turn_request_rejects_missing_security_snapshot() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_restore_missing_security";
        let thread_id = "thr_restore_missing_security";
        let turn_id = "turn_restore_missing_security";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_restore_missing_security",
            None,
        )
        .await;
        persist_test_runtime_snapshot_only(crud_store.as_ref(), workspace_id, thread_id, turn_id)
            .await;

        let timestamp = chrono::Utc::now().fixed_offset();
        let window = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 1,
                    tool_call_count: 1,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "stale_window_1"}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("running window should persist");

        let lookup = coordinator
            .restored_recovery_turn_request(thread_id, turn_id, 1_700_000_010)
            .await
            .expect("restored request lookup should evaluate");
        assert!(matches!(
            lookup,
            super::RestoredRecoveryTurnRequestLookup::Unavailable(
                super::RestoredRecoveryTurnUnavailable::MissingExecutionSecuritySnapshot
            )
        ));

        let still_running = crud_store
            .get_turn_execution_window(window.id.as_str())
            .await
            .expect("window read should succeed")
            .expect("window should exist");
        assert_eq!(still_running.status, ExecutionWindowStatus::Running);
    }

    #[tokio::test]
    async fn tool_item_recovery_restores_turn_from_runtime_snapshot_when_loop_missing() {
        let (crud_store, _agent_manager, coordinator) = setup_coordinator_with_agent().await;
        let workspace_id = "ws_tool_restore";
        let thread_id = "thr_tool_restore";
        let turn_id = "turn_tool_restore";
        let item_id = "tool_restore";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            None,
        )
        .await;
        persist_test_runtime_snapshot(crud_store.as_ref(), workspace_id, thread_id, turn_id).await;
        let job = crud_store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                item_id.to_owned(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::RetryAttempt,
                Some("tool idle timeout".to_owned()),
                None,
                None,
                None,
                0,
                2,
                serde_json::json!({}),
                serde_json::json!({
                    "base_backoff_secs": 0,
                    "max_wall_clock_secs": 60,
                    "no_progress_limit": 3,
                }),
                1_700_000_010,
            )
            .await
            .expect("tool recovery job should enqueue");

        let events = coordinator
            .run_ready_jobs(1_700_000_011, 1)
            .await
            .expect("tool recovery should run");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RetryAttemptStarted {
                job_id,
                turn_id: event_turn_id,
                item_id: event_item_id,
                item_type,
                attempt_number,
            }] if job_id == &job.id
                && event_turn_id == turn_id
                && event_item_id == item_id
                && *item_type == TurnItemType::WebFetch
                && *attempt_number == 1
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .expect("job should reload")
            .expect("job should exist");
        assert_eq!(reloaded.status, RecoveryJobStatus::Active);
    }

    #[tokio::test]
    async fn incomplete_in_flight_tool_history_blocks_before_restored_provider_execution() {
        let (crud_store, agent_manager, coordinator) = setup_coordinator_with_agent().await;
        let workspace_id = "ws_tool_uncertain";
        let thread_id = "thr_tool_uncertain";
        let turn_id = "turn_tool_uncertain";
        let item_id = "tool_uncertain";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            None,
        )
        .await;
        persist_test_runtime_snapshot(crud_store.as_ref(), workspace_id, thread_id, turn_id).await;

        let assistant_round = ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            Some("reasoning that produced the side effect"),
            vec![ProviderToolCall {
                id: item_id.to_owned(),
                name: "web_fetch".to_owned(),
                arguments: r#"{"url":"https://example.com"}"#.to_owned(),
            }],
            Some(ProviderReplayState::new(
                "deepseek",
                serde_json::json!({"reasoning_content": "reasoning that produced the side effect"}),
            )),
        );
        crud_store
            .insert_turn_llm_context(NewTurnLlmContextEntry {
                turn_id: turn_id.to_owned(),
                item_id: Some("reasoning_uncertain".to_owned()),
                attempt_id: None,
                sequence: 1,
                source: "assistant_round".to_owned(),
                tool_name: None,
                payload: serde_json::to_string(&assistant_round).unwrap(),
                output_policy_snapshot: serde_json::json!({}).to_string(),
                created_at: chrono::Utc::now().fixed_offset(),
                expires_at: None,
            })
            .await
            .expect("in-flight assistant round should persist");

        let job = crud_store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                item_id.to_owned(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::RetryAttempt,
                Some("tool outcome became uncertain during process loss".to_owned()),
                None,
                None,
                None,
                0,
                2,
                serde_json::json!({}),
                serde_json::json!({
                    "base_backoff_secs": 0,
                    "max_wall_clock_secs": 60,
                    "no_progress_limit": 3,
                }),
                1_700_000_010,
            )
            .await
            .expect("tool recovery job should enqueue");

        let events = coordinator
            .run_ready_jobs(1_700_000_011, 1)
            .await
            .expect("unsafe recovery should be handled deterministically");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoveryBlocked {
                job_id,
                turn_id: event_turn_id,
                reason,
            }] if job_id == &job.id
                && event_turn_id == turn_id
                && reason.contains("provider history")
                && reason.contains("incomplete")
        ));
        assert!(
            !agent_manager.has_thread(thread_id).await,
            "an incomplete in-flight tool round must be blocked before a provider loop can restart it"
        );
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .expect("job should reload")
            .expect("job should exist");
        assert_eq!(reloaded.status, RecoveryJobStatus::Blocked);
    }

    #[tokio::test]
    async fn tool_item_recovery_blocks_when_loop_missing_and_runtime_snapshot_missing() {
        let (crud_store, _agent_manager, coordinator) = setup_coordinator_with_agent().await;
        let workspace_id = "ws_tool_missing_snapshot";
        let thread_id = "thr_tool_missing_snapshot";
        let turn_id = "turn_tool_missing_snapshot";
        let item_id = "tool_missing_snapshot";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            None,
        )
        .await;
        let job = crud_store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                item_id.to_owned(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::RetryAttempt,
                Some("tool idle timeout".to_owned()),
                None,
                None,
                None,
                0,
                2,
                serde_json::json!({}),
                serde_json::json!({
                    "base_backoff_secs": 0,
                    "max_wall_clock_secs": 60,
                    "no_progress_limit": 3,
                }),
                1_700_000_010,
            )
            .await
            .expect("tool recovery job should enqueue");

        let events = coordinator
            .run_ready_jobs(1_700_000_011, 1)
            .await
            .expect("tool recovery should block without a restorable snapshot");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoveryBlocked {
                job_id,
                turn_id: event_turn_id,
                reason,
            }] if job_id == &job.id
                && event_turn_id == turn_id
                && reason.contains("runtime snapshot is missing")
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .expect("job should reload")
            .expect("job should exist");
        assert_eq!(reloaded.status, RecoveryJobStatus::Blocked);
        assert!(reloaded.active_attempt_id.is_none());
        assert!(
            reloaded
                .last_error
                .as_deref()
                .unwrap_or_default()
                .contains("runtime snapshot is missing")
        );
    }

    #[tokio::test]
    async fn startup_recovery_context_loads_latest_checkpoint_for_in_progress_turn() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_startup_checkpoint";
        let thread_id = "thr_startup_checkpoint";
        let turn_id = "turn_startup_checkpoint";
        let item_id = "item_startup_checkpoint";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            None,
        )
        .await;

        let timestamp = chrono::Utc::now().fixed_offset();
        let first_window = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({"runtimeWindowId": "runtime_window_1"}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("first window should persist");
        crud_store
            .mark_turn_execution_window_exhausted(
                first_window.id.as_str(),
                ExecutionWindowExhaustionReason::MaxToolCallsPerWindow,
                TurnExecutionWindowStatsRecord {
                    agent_round_count: 3,
                    tool_call_count: 5,
                    provider_token_count: 8,
                    metadata_json: serde_json::json!({}),
                    completed_at: timestamp + chrono::Duration::seconds(1),
                    updated_at: timestamp + chrono::Duration::seconds(1),
                },
            )
            .await
            .expect("window should mark exhausted");

        let payload = build_execution_checkpoint_payload(
            workspace_id.to_owned(),
            thread_id.to_owned(),
            turn_id.to_owned(),
            pioneer_protocol::ExecutionCheckpointOriginalRequestSummary {
                input_count: 1,
                text_preview: Some("startup recovery request".to_owned()),
                text_truncated: false,
                attachment_count: 0,
                attachment_kinds: Vec::new(),
            },
            pioneer_protocol::ExecutionCheckpointWindowSummary {
                window_id: Some("runtime_window_1".to_owned()),
                window_index: 1,
                started_at_unix_ms: Some(1_000),
                completed_at_unix_ms: Some(2_000),
                agent_round_count: 3,
                tool_call_count: 5,
                provider_token_count: Some(8),
                exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxToolCallsPerWindow),
            },
            pioneer_protocol::ExecutionCheckpointProviderBudgetSummary {
                model: Some("test-model".to_owned()),
                model_provider: Some("echo".to_owned()),
                agent_round_count: 3,
                tool_call_count: 5,
                provider_token_count: Some(8),
                provider_usage_available: true,
                exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxToolCallsPerWindow),
                exhausted_limit: Some(4),
                exhausted_observed: Some(5),
            },
            pioneer_protocol::ExecutionCheckpointToolSummary {
                requested_count: 5,
                executed_count: 5,
                unexecuted_count: 0,
                total_count: 5,
                succeeded_count: 4,
                failed_count: 1,
                in_progress_count: 0,
                detail_limit: 0,
                details_truncated: false,
                details: Vec::new(),
            },
            Vec::new(),
        );
        let checkpoint = crud_store
            .save_turn_execution_checkpoint(NewTurnExecutionCheckpointRecord {
                id: None,
                window_id: first_window.id.clone(),
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                checkpoint_kind: TurnExecutionCheckpointKind::WindowExhausted,
                payload_json: serde_json::to_value(&payload).expect("checkpoint should serialize"),
                created_at: timestamp + chrono::Duration::seconds(2),
            })
            .await
            .expect("checkpoint should persist");
        crud_store
            .mark_turn_execution_window_checkpointed(
                first_window.id.as_str(),
                timestamp + chrono::Duration::seconds(3),
            )
            .await
            .expect("window should mark checkpointed");

        let context = coordinator
            .execution_checkpoint_context_for_turn(thread_id, turn_id)
            .await
            .expect("checkpoint context lookup should succeed")
            .expect("checkpoint context should exist");
        assert_eq!(context.window_id, "runtime_window_1");
        assert_eq!(context.window_index, 1);
        assert_eq!(context.next_window_index(), 2);
        assert_eq!(context.checkpoint_id, checkpoint.id);
        assert_eq!(context.checkpoint_kind, "window_exhausted");
        assert_eq!(context.payload.turn_id, turn_id);

        let missing_context = coordinator
            .execution_checkpoint_context_for_turn(thread_id, "missing_turn")
            .await
            .expect("missing checkpoint context lookup should not fail");
        assert!(missing_context.is_none());

        let no_checkpoint_turn_id = "turn_startup_no_checkpoint";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            no_checkpoint_turn_id,
            "item_startup_no_checkpoint",
            None,
        )
        .await;
        let no_checkpoint_window = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: no_checkpoint_turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("window without checkpoint should persist");
        crud_store
            .mark_turn_execution_window_exhausted(
                no_checkpoint_window.id.as_str(),
                ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow,
                TurnExecutionWindowStatsRecord {
                    agent_round_count: 2,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({}),
                    completed_at: timestamp + chrono::Duration::seconds(4),
                    updated_at: timestamp + chrono::Duration::seconds(4),
                },
            )
            .await
            .expect("window without checkpoint should mark exhausted");
        let missing_checkpoint_context = coordinator
            .execution_checkpoint_context_for_turn(thread_id, no_checkpoint_turn_id)
            .await
            .expect("missing checkpoint for latest window should not fail");
        assert!(missing_checkpoint_context.is_none());
    }

    #[tokio::test]
    async fn startup_recovery_job_starts_restored_turn_from_checkpoint() {
        let (crud_store, agent_manager, coordinator) = setup_coordinator_with_agent().await;
        let workspace_id = "ws_startup_restore";
        let thread_id = "thr_startup_restore";
        let turn_id = "turn_startup_restore";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "item_startup_restore",
            None,
        )
        .await;
        persist_test_runtime_snapshot(crud_store.as_ref(), workspace_id, thread_id, turn_id).await;

        let timestamp = chrono::Utc::now().fixed_offset();
        let window = crud_store
            .create_turn_execution_window(
                NewTurnExecutionWindowRecord {
                    workspace_id: workspace_id.to_owned(),
                    thread_id: thread_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    window_index: 1,
                    status: ExecutionWindowStatus::Running,
                    exhaustion_reason: None,
                    agent_round_count: 0,
                    tool_call_count: 0,
                    provider_token_count: 0,
                    metadata_json: serde_json::json!({}),
                    started_at: timestamp,
                },
                timestamp,
                timestamp,
            )
            .await
            .expect("window should persist");
        crud_store
            .mark_turn_execution_window_exhausted(
                window.id.as_str(),
                ExecutionWindowExhaustionReason::MaxToolCallsPerWindow,
                TurnExecutionWindowStatsRecord {
                    agent_round_count: 1,
                    tool_call_count: 1,
                    provider_token_count: 1,
                    metadata_json: serde_json::json!({}),
                    completed_at: timestamp + chrono::Duration::seconds(1),
                    updated_at: timestamp + chrono::Duration::seconds(1),
                },
            )
            .await
            .expect("window should mark exhausted");
        let payload = build_execution_checkpoint_payload(
            workspace_id.to_owned(),
            thread_id.to_owned(),
            turn_id.to_owned(),
            pioneer_protocol::ExecutionCheckpointOriginalRequestSummary {
                input_count: 1,
                text_preview: Some("restore".to_owned()),
                text_truncated: false,
                attachment_count: 0,
                attachment_kinds: Vec::new(),
            },
            pioneer_protocol::ExecutionCheckpointWindowSummary {
                window_id: Some("restore_window_1".to_owned()),
                window_index: 1,
                started_at_unix_ms: Some(1),
                completed_at_unix_ms: Some(2),
                agent_round_count: 1,
                tool_call_count: 1,
                provider_token_count: Some(1),
                exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxToolCallsPerWindow),
            },
            pioneer_protocol::ExecutionCheckpointProviderBudgetSummary {
                model: Some("test-model".to_owned()),
                model_provider: Some("echo".to_owned()),
                agent_round_count: 1,
                tool_call_count: 1,
                provider_token_count: Some(1),
                provider_usage_available: true,
                exhaustion_reason: Some(ExecutionWindowExhaustionReason::MaxToolCallsPerWindow),
                exhausted_limit: Some(1),
                exhausted_observed: Some(1),
            },
            pioneer_protocol::ExecutionCheckpointToolSummary {
                requested_count: 1,
                executed_count: 1,
                unexecuted_count: 0,
                total_count: 1,
                succeeded_count: 1,
                failed_count: 0,
                in_progress_count: 0,
                detail_limit: 0,
                details_truncated: false,
                details: Vec::new(),
            },
            Vec::new(),
        );
        crud_store
            .save_turn_execution_checkpoint(NewTurnExecutionCheckpointRecord {
                id: None,
                window_id: window.id.clone(),
                workspace_id: workspace_id.to_owned(),
                thread_id: thread_id.to_owned(),
                turn_id: turn_id.to_owned(),
                checkpoint_kind: TurnExecutionCheckpointKind::WindowExhausted,
                payload_json: serde_json::to_value(&payload).expect("checkpoint should serialize"),
                created_at: timestamp + chrono::Duration::seconds(2),
            })
            .await
            .expect("checkpoint should persist");
        crud_store
            .mark_turn_execution_window_checkpointed(
                window.id.as_str(),
                timestamp + chrono::Duration::seconds(3),
            )
            .await
            .expect("window should mark checkpointed");
        coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_startup_restore".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(
                        ProviderFailureClass::MaxOutputTokens,
                        "window exhausted before restart",
                    ),
                },
                1_700_000_000,
            )
            .await
            .expect("provider failure job should enqueue");

        let events = coordinator
            .run_ready_jobs(1_700_000_001, 1)
            .await
            .expect("startup recovery job should run");

        assert!(events.iter().any(|event| matches!(
            event,
            RecoveryCoordinatorEvent::RetryAttemptStarted {
                turn_id: started_turn_id,
                item_type: TurnItemType::Reasoning,
                ..
            } if started_turn_id == turn_id
        )));
        assert!(
            agent_manager.has_thread(thread_id).await,
            "restored recovery should create the agent thread after restart"
        );
    }

    async fn claim_and_activate(crud_store: &CrudStore, job_id: &str) -> String {
        let active_attempt_id = "recovery_attempt_1".to_owned();
        let claimed = crud_store
            .claim_due_recovery_jobs(1_700_000_001, 45, 1)
            .await
            .expect("job should claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, job_id);
        let claim_token = claimed[0]
            .claim_token
            .as_deref()
            .expect("claimed job should have claim token");
        assert!(matches!(
            crud_store
                .mark_claimed_recovery_job_active(
                    job_id,
                    claim_token,
                    active_attempt_id.as_str(),
                    1_700_000_001,
                )
                .await
                .expect("job should enter active recovery"),
            ClaimedRecoveryActivation::Activated
        ));
        active_attempt_id
    }

    fn recovery_context(job_id: &str, attempt_id: &str) -> RecoveryAttemptContext {
        RecoveryAttemptContext {
            job_id: job_id.to_owned(),
            attempt_id: attempt_id.to_owned(),
        }
    }

    #[tokio::test]
    async fn provider_failure_inside_recovery_requeues_same_job() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_same_recovery_job";
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let active_attempt_id = claim_and_activate(crud_store.as_ref(), job.id.as_str()).await;

        let events = coordinator
            .record_recovery_provider_failure(
                job.id.as_str(),
                active_attempt_id.as_str(),
                provider_failure(
                    ProviderFailureClass::NetworkTransient,
                    "failed inside recovery",
                ),
                1_700_000_002,
            )
            .await
            .expect("active recovery failure should be recorded");

        assert_eq!(
            crud_store
                .count_recovery_jobs_for_turn(turn_id)
                .await
                .unwrap(),
            1
        );
        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RetryScheduled { job_id, .. }] if job_id == &job.id
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Pending);
        assert_eq!(reloaded.run_count, 1);
    }

    #[tokio::test]
    async fn timeout_inside_recovery_requeues_same_job() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_timeout_recovery_job";
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "original_reasoning".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let _active_attempt_id = claim_and_activate(crud_store.as_ref(), job.id.as_str()).await;

        let result = coordinator
            .record_recovery_timeout_failure(
                &timeout_candidate(
                    "attempt_timeout_inside_recovery",
                    turn_id,
                    "recovery_reasoning",
                    TurnItemType::Reasoning,
                ),
                1_700_000_002,
            )
            .await
            .expect("timeout inside active recovery should be recorded")
            .expect("active recovery job should be used");

        assert_eq!(result.0, job.id);
        assert_eq!(
            crud_store
                .count_recovery_jobs_for_turn(turn_id)
                .await
                .unwrap(),
            1
        );
        assert!(matches!(
            result.1.as_slice(),
            [RecoveryCoordinatorEvent::RetryScheduled { job_id, .. }] if job_id == &job.id
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Pending);
        assert_eq!(reloaded.run_count, 1);
    }

    #[tokio::test]
    async fn new_failure_reuses_existing_open_recovery_for_turn() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_reuses_open_recovery";
        let first_outcome = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("first provider failure should enqueue");
        assert!(matches!(
            first_outcome,
            RecoveryJobEnqueueOutcome::Created(_)
        ));
        let first = first_outcome.into_job();

        let second_outcome = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_2".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::Provider5xx, "second"),
                },
                1_700_000_001,
            )
            .await
            .expect("second provider failure should reuse open recovery");
        assert!(matches!(
            second_outcome,
            RecoveryJobEnqueueOutcome::Reused(_)
        ));
        let second = second_outcome.into_job();

        assert_eq!(second.id, first.id);
        assert_eq!(
            crud_store
                .count_recovery_jobs_for_turn(turn_id)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn timeout_reuses_existing_open_recovery_for_turn() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_timeout_reuses_open_recovery";
        let first = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("first provider failure should enqueue")
            .into_job();

        let second_outcome = coordinator
            .enqueue_timeout_job(
                &timeout_candidate(
                    "attempt_timeout_reuse",
                    turn_id,
                    "reasoning_2",
                    TurnItemType::Reasoning,
                ),
                1_700_000_001,
            )
            .await
            .expect("timeout should reuse open recovery");

        assert!(matches!(
            second_outcome,
            RecoveryJobEnqueueOutcome::Reused(_)
        ));
        assert_eq!(second_outcome.into_job().id, first.id);
        assert_eq!(
            crud_store
                .count_recovery_jobs_for_turn(turn_id)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn tool_timeout_recovery_uses_persisted_tool_policy_snapshot() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_tool_policy_snapshot";
        let item_id = "web_fetch_tool_policy";
        let snapshot = tool_snapshot(
            ToolRecoveryRetryClass::Network,
            ToolRecoveryIdempotencyMode::Safe,
            6,
            RecoveryAction::RetryWithBackoff,
        );
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            "ws_tool_policy_snapshot",
            "thr_tool_policy_snapshot",
            turn_id,
            item_id,
            Some(snapshot.clone()),
        )
        .await;

        let job = coordinator
            .enqueue_timeout_job(
                &timeout_candidate(
                    "attempt_tool_policy_snapshot",
                    turn_id,
                    item_id,
                    TurnItemType::WebFetch,
                ),
                1_700_000_010,
            )
            .await
            .expect("timeout should enqueue recovery")
            .into_job();

        assert_eq!(job.action, RecoveryAction::RetryWithBackoff);
        assert_eq!(job.max_attempts, 6);
        assert_eq!(
            job.policy_snapshot
                .get("policy_source")
                .and_then(|value| value.as_str()),
            Some("tool_item_snapshot")
        );
        assert_eq!(
            job.policy_snapshot
                .get("retry_class")
                .and_then(|value| value.as_str()),
            Some("network")
        );
        assert_eq!(
            job.policy_snapshot
                .get("idempotency_mode")
                .and_then(|value| value.as_str()),
            Some("safe")
        );
        assert_eq!(
            job.policy_snapshot
                .get("base_backoff_secs")
                .and_then(|value| value.as_u64()),
            Some(7)
        );
        assert_eq!(
            job.policy_snapshot
                .get("max_wall_clock_secs")
                .and_then(|value| value.as_u64()),
            Some(777)
        );
        assert_eq!(
            job.policy_snapshot
                .get("no_progress_limit")
                .and_then(|value| value.as_i64()),
            Some(9)
        );
        assert_eq!(
            job.policy_snapshot
                .get("tool_recovery_policy")
                .and_then(|value| value.get("maxAttempts"))
                .and_then(|value| value.as_u64()),
            Some(6)
        );
    }

    #[tokio::test]
    async fn tool_timeout_without_snapshot_uses_deterministic_conservative_policy() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_tool_missing_policy_snapshot";
        let item_id = "web_fetch_missing_policy";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            "ws_tool_missing_policy_snapshot",
            "thr_tool_missing_policy_snapshot",
            turn_id,
            item_id,
            None,
        )
        .await;

        let job = coordinator
            .enqueue_timeout_job(
                &timeout_candidate(
                    "attempt_tool_missing_policy",
                    turn_id,
                    item_id,
                    TurnItemType::WebFetch,
                ),
                1_700_000_010,
            )
            .await
            .expect("timeout should enqueue conservative recovery")
            .into_job();

        assert_eq!(job.action, RecoveryAction::BlockResumable);
        assert_eq!(job.max_attempts, 0);
        assert_eq!(
            job.policy_snapshot
                .get("policy_source")
                .and_then(|value| value.as_str()),
            Some("tool_item_missing_snapshot")
        );
        assert!(job.policy_snapshot.get("retry_class").is_none());
        assert_eq!(
            job.policy_snapshot
                .get("max_wall_clock_secs")
                .and_then(|value| value.as_u64()),
            Some(240)
        );
        assert_eq!(
            job.policy_snapshot
                .get("no_progress_limit")
                .and_then(|value| value.as_i64()),
            Some(0)
        );
    }

    #[tokio::test]
    async fn backfill_does_not_enqueue_recovery_for_terminal_turn() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let timestamp = 1_700_000_000;
        workspace::ActiveModel {
            id: Set("ws_terminal_backfill".to_owned()),
            name: Set("Terminal backfill".to_owned()),
            is_active: Set(true),
            is_current: Set(true),
            ..Default::default()
        }
        .insert(&crud_store.database_connection())
        .await
        .expect("workspace should persist");
        let thread = Thread {
            workspace_id: "ws_terminal_backfill".to_owned(),
            id: "thr_terminal_backfill".to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "test-model".to_owned(),
            model_provider: "echo".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = pioneer_protocol::Turn {
            id: "turn_terminal_backfill".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        let item_id = "reasoning_terminal_backfill";

        crud_store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[UserInput::Text {
                    text: "hello".to_owned(),
                    text_elements: Vec::new(),
                }],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("turn start should persist");
        crud_store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: TurnItem::Reasoning {
                        id: item_id.to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
                timestamp + 1,
            )
            .await
            .expect("item start should create running attempt");
        crud_store
            .configure_turn_item_attempt_deadlines(
                turn.id.as_str(),
                item_id,
                timestamp + 1,
                Some(timestamp + 2),
                Some(timestamp + 2),
                Some(timestamp + 2),
            )
            .await
            .expect("deadlines should be configured");

        let candidates = crud_store
            .list_timeout_candidates(timestamp + 3, 1)
            .await
            .expect("timeout candidate query should succeed");
        assert_eq!(candidates.len(), 1);
        assert!(
            crud_store
                .transition_timeout_candidate(&candidates[0], timestamp + 3)
                .await
                .expect("timeout transition should succeed")
        );

        crud_store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn: pioneer_protocol::Turn {
                        id: turn.id.clone(),
                        status: TurnStatus::Completed,
                        turn_kind: Default::default(),
                        origin: Default::default(),
                        mode: Default::default(),
                        author: None,
                        reply_to_turn_id: None,
                        mentions: Vec::new(),
                        message_revision: 0,
                        message_deleted: false,
                        error: None,
                        prompt_manifest: None,
                        permission_profile:
                            pioneer_protocol::default_turn_permission_profile_snapshot(),
                    },
                },
                timestamp + 4,
            )
            .await
            .expect("turn completion should persist");

        let events = coordinator
            .run_ready_jobs(timestamp + 5, 64)
            .await
            .expect("recovery worker should run");

        assert!(events.is_empty());
        assert_eq!(
            crud_store
                .count_recovery_jobs_for_turn(turn.id.as_str())
                .await
                .unwrap(),
            0
        );
        assert!(
            crud_store
                .list_unqueued_timeout_candidates(64)
                .await
                .expect("backfill candidates should reload")
                .is_empty(),
            "terminal-turn timeout should be marked non-recoverable"
        );
    }

    #[tokio::test]
    async fn timeout_backfill_suppresses_recovery_when_turn_progressed_after_item() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let timestamp = 1_700_000_000;
        let thread = Thread {
            workspace_id: "ws_timeout_liveness".to_owned(),
            id: "thr_timeout_liveness".to_owned(),
            name: None,
            preview: String::new(),
            mode: ThreadMode::Agent,
            model: "test-model".to_owned(),
            model_provider: "echo".to_owned(),
            reasoning_effort: None,
            created_at: timestamp,
            updated_at: timestamp,
            status: ThreadStatus::Active,
            origin_kind: ThreadOriginKind::User,
            sidebar_visibility: ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };
        let turn = pioneer_protocol::Turn {
            id: "turn_timeout_liveness".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        let stale_item_id = "reasoning_stale_frontier";
        let later_item_id = "agent_message_after_stale_reasoning";

        crud_store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[UserInput::Text {
                    text: "continue".to_owned(),
                    text_elements: Vec::new(),
                }],
                pioneer_protocol::PersistedActorRef::System,
            )
            .await
            .expect("turn start should persist");
        crud_store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: TurnItem::Reasoning {
                        id: stale_item_id.to_owned(),
                        summary: Vec::new(),
                        content: Vec::new(),
                    },
                },
                timestamp + 1,
            )
            .await
            .expect("stale item start should persist");
        crud_store
            .configure_turn_item_attempt_deadlines(
                turn.id.as_str(),
                stale_item_id,
                timestamp + 1,
                Some(timestamp + 2),
                Some(timestamp + 2),
                Some(timestamp + 2),
            )
            .await
            .expect("deadlines should be configured");
        let later_item = TurnItem::AgentMessage {
            id: later_item_id.to_owned(),
            text: "still working".to_owned(),
            phase: AgentMessagePhase::FinalAnswer,
            markdown: None,
            markdown_version: None,
        };
        crud_store
            .materialize_item_started(
                ItemStartedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: later_item.clone(),
                },
                timestamp + 10,
            )
            .await
            .expect("later item start should persist");
        crud_store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: thread.workspace_id.clone(),
                    thread_id: thread.id.clone(),
                    turn_id: turn.id.clone(),
                    item: later_item,
                },
                timestamp + 11,
            )
            .await
            .expect("later item completion should persist");

        let candidates = crud_store
            .list_timeout_candidates(timestamp + 12, 8)
            .await
            .expect("timeout candidate query should succeed");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].item_id, stale_item_id);
        assert!(
            crud_store
                .transition_timeout_candidate(&candidates[0], timestamp + 12)
                .await
                .expect("timeout transition should succeed")
        );

        let events = coordinator
            .run_ready_jobs(timestamp + 13, 64)
            .await
            .expect("recovery worker should run");

        assert!(
            events.is_empty(),
            "live turn item timeout must not enqueue recovery"
        );
        assert_eq!(
            crud_store
                .count_recovery_jobs_for_turn(turn.id.as_str())
                .await
                .unwrap(),
            0
        );
        assert!(
            crud_store
                .list_unqueued_timeout_candidates(64)
                .await
                .expect("backfill candidates should reload")
                .is_empty(),
            "suppressed timeout should not be backfilled into recovery later"
        );
    }

    #[tokio::test]
    async fn late_provider_failure_after_timeout_is_stale_noop() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_late_provider_failure";
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "original_reasoning".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let active_attempt_id = claim_and_activate(crud_store.as_ref(), job.id.as_str()).await;

        let timeout_events = coordinator
            .record_recovery_timeout_failure(
                &timeout_candidate(
                    "attempt_timeout_before_late_provider",
                    turn_id,
                    "recovery_reasoning",
                    TurnItemType::Reasoning,
                ),
                1_700_000_002,
            )
            .await
            .expect("timeout inside active recovery should be recorded")
            .expect("active recovery job should be used")
            .1;
        assert!(matches!(
            timeout_events.as_slice(),
            [RecoveryCoordinatorEvent::RetryScheduled { job_id, .. }] if job_id == &job.id
        ));

        let late_events = coordinator
            .record_recovery_provider_failure(
                job.id.as_str(),
                active_attempt_id.as_str(),
                provider_failure(
                    ProviderFailureClass::NetworkTransient,
                    "late provider failure after timeout",
                ),
                1_700_000_003,
            )
            .await
            .expect("late stale provider failure should not error");
        assert!(late_events.is_empty());

        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Pending);
        assert_eq!(reloaded.run_count, 1);
        assert!(reloaded.active_attempt_id.is_none());
    }

    #[tokio::test]
    async fn active_recovery_watchdog_keeps_attempt_within_wall_clock_budget() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_active_recovery_within_budget";
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let active_attempt_id = claim_and_activate(crud_store.as_ref(), job.id.as_str()).await;

        let events = coordinator
            .run_ready_jobs(1_700_000_180, 64)
            .await
            .expect("active watchdog should run");

        assert!(events.is_empty());
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Active);
        assert_eq!(
            reloaded.active_attempt_id.as_deref(),
            Some(active_attempt_id.as_str())
        );
    }

    #[tokio::test]
    async fn active_recovery_watchdog_does_not_use_hidden_three_minute_ceiling() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_active_recovery_watchdog";
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let active_attempt_id = claim_and_activate(crud_store.as_ref(), job.id.as_str()).await;

        // The provider-failure policy allows a 15-minute recovery episode.
        // Crossing the historical three-minute implementation detail must not
        // retry or terminalize a still-owned attempt by itself.
        let events = coordinator
            .run_ready_jobs(1_700_000_182, 64)
            .await
            .expect("active watchdog should inspect the still-live attempt");

        assert!(events.is_empty());
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Active);
        assert_eq!(reloaded.run_count, 0);
        assert_eq!(
            reloaded.active_attempt_id.as_deref(),
            Some(active_attempt_id.as_str())
        );
    }

    #[test]
    fn periodic_item_heartbeat_is_not_recovery_progress() {
        assert!(!is_causal_recovery_progress("item/heartbeat"));
        assert!(!is_causal_recovery_progress("runtime/observed_in_progress"));
        assert!(is_causal_recovery_progress("item/snapshot_updated"));
        assert!(is_causal_recovery_progress(
            "turn/execution_window_continued"
        ));
    }

    #[tokio::test]
    async fn active_recovery_watchdog_exhausts_job_after_total_wall_clock_budget() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_active_recovery_total_budget";
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let _active_attempt_id = claim_and_activate(crud_store.as_ref(), job.id.as_str()).await;

        let events = coordinator
            .run_ready_jobs(1_700_000_901, 64)
            .await
            .expect("active watchdog should expire the recovery job");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoveryExhausted(outcome)]
                if outcome.job_id == job.id && outcome.status == RecoveryJobStatus::Exhausted
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Exhausted);
        assert_eq!(reloaded.run_count, 1);
        assert!(reloaded.active_attempt_id.is_none());
    }

    #[tokio::test]
    async fn terminal_active_recovery_cancels_pending_jobs_before_claiming_more_work() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_terminal_active_cancels_pending";
        let active_job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let _active_attempt_id =
            claim_and_activate(crud_store.as_ref(), active_job.id.as_str()).await;
        let pending_job = crud_store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "reasoning_duplicate".to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::ProviderError,
                RecoveryAction::RetryWithBackoff,
                Some("duplicate pending".to_owned()),
                Some(ProviderFailureClass::NetworkTransient),
                None,
                None,
                0,
                3,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_000,
            )
            .await
            .expect("duplicate pending job should enqueue for regression setup");

        let events = coordinator
            .run_ready_jobs(1_700_000_901, 64)
            .await
            .expect("active watchdog should expire the recovery job");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoveryExhausted(outcome)]
                if outcome.job_id == active_job.id
        ));
        let pending = crud_store
            .get_recovery_job(pending_job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.status, RecoveryJobStatus::Cancelled);
    }

    #[tokio::test]
    async fn claimed_pending_job_does_not_terminalize_while_turn_has_active_recovery() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_pending_waits_for_active_recovery";
        let active_job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_active".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let active_attempt_id =
            claim_and_activate(crud_store.as_ref(), active_job.id.as_str()).await;
        let pending_terminal_job = crud_store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "reasoning_terminal_duplicate".to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::ProviderError,
                RecoveryAction::MarkFailed,
                Some("duplicate terminal pending".to_owned()),
                Some(ProviderFailureClass::InvalidRequest),
                None,
                None,
                0,
                0,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_001,
            )
            .await
            .expect("duplicate terminal pending job should enqueue for regression setup");

        let events = coordinator
            .run_ready_jobs(1_700_000_002, 64)
            .await
            .expect("pending job should wait behind active recovery");

        assert!(events.is_empty());
        let active = crud_store
            .get_recovery_job(active_job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.status, RecoveryJobStatus::Active);
        assert_eq!(
            active.active_attempt_id.as_deref(),
            Some(active_attempt_id.as_str())
        );
        let pending = crud_store
            .get_recovery_job(pending_terminal_job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.status, RecoveryJobStatus::Pending);
        assert_eq!(pending.run_count, 0);
    }

    #[tokio::test]
    async fn provider_failure_inside_recovery_exhausts_same_job_when_attempts_are_spent() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: "turn_exhaust_recovery_job".to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::ModelNotFound, "missing"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let active_attempt_id = claim_and_activate(crud_store.as_ref(), job.id.as_str()).await;

        let events = coordinator
            .record_recovery_provider_failure(
                job.id.as_str(),
                active_attempt_id.as_str(),
                provider_failure(ProviderFailureClass::ModelNotFound, "still missing"),
                1_700_000_002,
            )
            .await
            .expect("active recovery failure should be recorded");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoveryExhausted(outcome)] if outcome.job_id == job.id
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Exhausted);
        assert_eq!(reloaded.run_count, 1);
        assert_eq!(reloaded.provider_attempt_number, 1);
    }

    #[tokio::test]
    async fn turn_completion_marks_active_recovery_succeeded() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: "turn_success_recovery_job".to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let active_attempt_id = claim_and_activate(crud_store.as_ref(), job.id.as_str()).await;
        let recovery = recovery_context(job.id.as_str(), active_attempt_id.as_str());

        let events = coordinator
            .complete_active_recovery_for_turn(
                "turn_success_recovery_job",
                Some(&recovery),
                1_700_000_002,
            )
            .await
            .expect("turn completion should complete active recovery");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoverySucceeded { job_id, .. }] if job_id == &job.id
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Succeeded);
        assert_eq!(reloaded.run_count, 1);
        assert_eq!(reloaded.provider_attempt_number, 1);
    }

    #[tokio::test]
    async fn tool_recovery_success_closes_job_before_turn_completion() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_tool_recovery_success";
        let job = coordinator
            .enqueue_timeout_job(
                &timeout_candidate(
                    "attempt_tool_timeout",
                    turn_id,
                    "tool_1",
                    TurnItemType::DynamicToolCall,
                ),
                1_700_000_000,
            )
            .await
            .expect("timeout should enqueue recovery")
            .into_job();
        let active_attempt_id = claim_and_activate(crud_store.as_ref(), job.id.as_str()).await;
        let recovery = recovery_context(job.id.as_str(), active_attempt_id.as_str());

        let events = coordinator
            .succeed_active_recovery_attempt(turn_id, &recovery, 1_700_000_002)
            .await
            .expect("tool recovery success should close active job");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoverySucceeded { job_id, .. }] if job_id == &job.id
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Succeeded);
        assert!(reloaded.active_attempt_id.is_none());

        let next = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_after_tool_recovery".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "new"),
                },
                1_700_000_003,
            )
            .await
            .expect("new failure should create a fresh recovery job");

        assert!(matches!(next, RecoveryJobEnqueueOutcome::Created(_)));
        assert_eq!(
            crud_store
                .count_recovery_jobs_for_turn(turn_id)
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn turn_completion_cancels_other_pending_recovery_jobs() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_completion_cancels_pending";
        let active_job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let active_attempt_id =
            claim_and_activate(crud_store.as_ref(), active_job.id.as_str()).await;

        let stale_pending = crud_store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "reasoning_stale".to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::ProviderError,
                RecoveryAction::RetryWithBackoff,
                Some("stale duplicate".to_owned()),
                None,
                None,
                None,
                0,
                3,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_001,
            )
            .await
            .expect("stale pending job should enqueue for regression setup");

        let recovery = recovery_context(active_job.id.as_str(), active_attempt_id.as_str());
        let events = coordinator
            .complete_active_recovery_for_turn(turn_id, Some(&recovery), 1_700_000_002)
            .await
            .expect("turn completion should complete active recovery");
        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoverySucceeded { job_id, .. }] if job_id == &active_job.id
        ));

        let stale = crud_store
            .get_recovery_job(stale_pending.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stale.status, RecoveryJobStatus::Cancelled);
    }

    #[tokio::test]
    async fn turn_block_marks_active_recovery_blocked_without_exhausted_event() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_block_blocks_recovery";
        let active_job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::NetworkTransient, "first"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        let active_attempt_id =
            claim_and_activate(crud_store.as_ref(), active_job.id.as_str()).await;

        let stale_pending = crud_store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "reasoning_stale".to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::ProviderError,
                RecoveryAction::RetryWithBackoff,
                Some("stale duplicate".to_owned()),
                None,
                None,
                None,
                0,
                3,
                serde_json::json!({}),
                serde_json::json!({}),
                1_700_000_001,
            )
            .await
            .expect("stale pending job should enqueue for regression setup");

        let recovery = recovery_context(active_job.id.as_str(), active_attempt_id.as_str());
        let events = coordinator
            .block_active_recoveries_for_turn(
                turn_id,
                Some(&recovery),
                "total window budget exhausted",
                1_700_000_002,
            )
            .await
            .expect("turn block should close active recovery");
        assert!(
            events.is_empty(),
            "blocked recovery must not emit failed/exhausted recovery events"
        );

        let active = crud_store
            .get_recovery_job(active_job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(active.status, RecoveryJobStatus::Blocked);
        assert_eq!(
            active.last_error.as_deref(),
            Some("total window budget exhausted")
        );
        assert!(active.active_attempt_id.is_none());

        let stale = crud_store
            .get_recovery_job(stale_pending.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stale.status, RecoveryJobStatus::Cancelled);
        assert_eq!(
            stale.last_error.as_deref(),
            Some("turn blocked; pending recovery jobs cancelled")
        );
    }

    #[tokio::test]
    async fn active_transient_recovery_uses_persisted_policy_snapshot() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let turn_id = "turn_persisted_policy_snapshot";
        let job = crud_store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "reasoning_1".to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::ProviderError,
                RecoveryAction::RetryWithBackoff,
                Some("persisted policy regression".to_owned()),
                Some(ProviderFailureClass::NetworkTransient),
                None,
                None,
                0,
                5,
                serde_json::json!({
                    "base_backoff_secs": 7,
                    "max_attempts": 5,
                    "max_wall_clock_secs": 300,
                    "no_progress_limit": 5,
                }),
                serde_json::json!({
                    "base_backoff_secs": 7,
                    "max_attempts": 5,
                    "max_wall_clock_secs": 300,
                    "no_progress_limit": 5,
                }),
                1_700_000_000,
            )
            .await
            .expect("job should enqueue with persisted policy");
        let active_attempt_id = claim_and_activate(crud_store.as_ref(), job.id.as_str()).await;

        let events = coordinator
            .record_recovery_provider_failure(
                job.id.as_str(),
                active_attempt_id.as_str(),
                provider_failure(ProviderFailureClass::NetworkTransient, "connection reset"),
                1_700_000_002,
            )
            .await
            .expect("active recovery failure should use persisted policy");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RetryScheduled { job_id, .. }] if job_id == &job.id
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Pending);
        assert_eq!(reloaded.run_count, 1);
    }

    #[tokio::test]
    async fn non_retryable_provider_failure_inside_active_recovery_is_not_rescheduled() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: "turn_active_then_invalid".to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(
                        ProviderFailureClass::NetworkTransient,
                        "connection reset",
                    ),
                },
                1_700_000_000,
            )
            .await
            .expect("transient provider failure should enqueue")
            .into_job();
        let active_attempt_id = claim_and_activate(crud_store.as_ref(), job.id.as_str()).await;

        let events = coordinator
            .record_recovery_provider_failure(
                job.id.as_str(),
                active_attempt_id.as_str(),
                provider_failure(
                    ProviderFailureClass::ProviderRejected,
                    "HTTP 400 unchanged request",
                ),
                1_700_000_002,
            )
            .await
            .expect("request rejection should terminalize active recovery");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoveryExhausted(outcome)]
                if outcome.job_id == job.id
                    && outcome.status == RecoveryJobStatus::Failed
                    && outcome.error_message.contains("will not be sent again")
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Failed);
        assert_eq!(reloaded.run_count, 1);
    }

    #[tokio::test]
    async fn invalid_request_provider_policy_marks_failed_without_retry() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: "turn_invalid_request_recovery_job".to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::InvalidRequest, "bad request"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();
        assert_eq!(job.action, RecoveryAction::MarkFailed);
        assert_eq!(job.max_attempts, 0);

        let events = coordinator
            .run_ready_jobs(1_700_000_001, 1)
            .await
            .expect("invalid-request recovery job should run");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoveryExhausted(outcome)]
                if outcome.job_id == job.id && outcome.status == RecoveryJobStatus::Failed
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Failed);
        assert_eq!(reloaded.run_count, 0);
    }

    #[tokio::test]
    async fn provider_rejected_policy_marks_failed_without_retry() {
        let (_crud_store, coordinator) = setup_coordinator().await;
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: "turn_provider_rejected_fallback".to_owned(),
                    item_id: "reasoning_provider_rejected".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(
                        ProviderFailureClass::ProviderRejected,
                        "provider rejected request",
                    ),
                },
                1_700_000_000,
            )
            .await
            .expect("provider rejection should enqueue terminal recovery")
            .into_job();

        assert_eq!(job.action, RecoveryAction::MarkFailed);
        assert_eq!(job.max_attempts, 0);
        assert_eq!(job.status, RecoveryJobStatus::Pending);
    }

    #[test]
    fn request_rejections_without_deterministic_mutation_are_non_retryable() {
        let registry = RecoveryPolicyRegistry::default();

        for class in [
            ProviderFailureClass::ProviderRejected,
            ProviderFailureClass::InvalidRequest,
            ProviderFailureClass::MalformedProviderRequest,
            ProviderFailureClass::UnsupportedParameter,
            ProviderFailureClass::UnsupportedCapability,
            ProviderFailureClass::Unknown,
        ] {
            let policy = registry.policy_for_provider_failure(class);
            assert_eq!(policy.action, RecoveryAction::MarkFailed);
            assert_eq!(policy.max_attempts, 0);
        }
    }

    #[tokio::test]
    async fn persisted_legacy_retry_policy_cannot_replay_rejected_request() {
        let (_crud_store, coordinator) = setup_coordinator().await;

        for class in [
            ProviderFailureClass::ProviderRejected,
            ProviderFailureClass::InvalidRequest,
            ProviderFailureClass::MalformedProviderRequest,
            ProviderFailureClass::UnsupportedParameter,
            ProviderFailureClass::UnsupportedCapability,
            ProviderFailureClass::Unknown,
        ] {
            let job = provider_plan_job(class, "stream");
            let plan = coordinator
                .build_attempt_plan(&job, 1)
                .await
                .expect("legacy provider plan should build");
            assert!(
                plan.terminal_reason.is_some(),
                "{class:?} must stop before provider execution"
            );
            assert!(!plan.force_non_stream);
            assert!(!plan.disable_tool_calling);
            assert!(!plan.disable_image_input);
            assert!(!plan.compact_history);
        }
    }

    #[test]
    fn deterministic_request_remediations_allow_only_one_attempt() {
        let registry = RecoveryPolicyRegistry::default();

        for class in [
            ProviderFailureClass::AuthExpired,
            ProviderFailureClass::AuthOrPermission,
            ProviderFailureClass::PromptTooLong,
            ProviderFailureClass::ContextTooLarge,
            ProviderFailureClass::UnsupportedStreaming,
            ProviderFailureClass::UnsupportedImageInput,
            ProviderFailureClass::UnsupportedToolCalling,
            ProviderFailureClass::PermissionDenied,
        ] {
            let policy = registry.policy_for_provider_failure(class);
            assert_ne!(policy.action, RecoveryAction::MarkFailed);
            assert_eq!(
                policy.max_attempts, 1,
                "{class:?} must get exactly one changed-request attempt"
            );
        }
    }

    #[tokio::test]
    async fn unsupported_streaming_requires_an_actual_transport_change() {
        let (_crud_store, coordinator) = setup_coordinator().await;

        let stream_plan = coordinator
            .build_attempt_plan(
                &provider_plan_job(ProviderFailureClass::UnsupportedStreaming, "stream"),
                1,
            )
            .await
            .expect("stream remediation plan should build");
        assert!(stream_plan.force_non_stream);
        assert!(stream_plan.terminal_reason.is_none());

        let non_stream_plan = coordinator
            .build_attempt_plan(
                &provider_plan_job(ProviderFailureClass::UnsupportedStreaming, "non_stream"),
                1,
            )
            .await
            .expect("non-stream remediation plan should build");
        assert!(!non_stream_plan.force_non_stream);
        assert!(non_stream_plan.terminal_reason.is_some());
    }

    #[tokio::test]
    async fn unsupported_tool_calling_plan_disables_tools_without_terminal_reason() {
        let (_crud_store, coordinator) = setup_coordinator().await;
        let job = provider_plan_job(ProviderFailureClass::UnsupportedToolCalling, "stream");

        let plan = coordinator
            .build_attempt_plan(&job, 1)
            .await
            .expect("unsupported tool-calling plan should build");

        assert!(plan.disable_tool_calling);
        assert!(plan.force_non_stream);
        assert!(!plan.disable_image_input);
        assert!(plan.terminal_reason.is_none());
    }

    #[tokio::test]
    async fn unsupported_image_input_plan_removes_images_without_terminal_reason() {
        let (_crud_store, coordinator) = setup_coordinator().await;
        let job = provider_plan_job(ProviderFailureClass::UnsupportedImageInput, "non_stream");

        let plan = coordinator
            .build_attempt_plan(&job, 1)
            .await
            .expect("unsupported image-input plan should build");

        assert!(plan.disable_image_input);
        assert!(!plan.disable_tool_calling);
        assert!(!plan.force_non_stream);
        assert!(plan.terminal_reason.is_none());
    }

    #[tokio::test]
    async fn model_not_found_without_explicit_fallback_blocks_recovery_instead_of_failing() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: "turn_model_not_found_blocks".to_owned(),
                    item_id: "reasoning_model_missing".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::ModelNotFound, "missing"),
                },
                1_700_000_000,
            )
            .await
            .expect("model-not-found should enqueue blocked recovery")
            .into_job();

        assert_eq!(job.action, RecoveryAction::BlockResumable);

        let events = coordinator
            .run_ready_jobs(1_700_000_001, 1)
            .await
            .expect("model-not-found recovery job should block");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoveryBlocked { job_id, reason, .. }]
                if job_id == &job.id && reason.contains("model_not_found")
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Blocked);
        assert!(
            reloaded
                .last_error
                .as_deref()
                .unwrap_or_default()
                .contains("model_not_found")
        );
    }

    #[tokio::test]
    async fn explicit_resume_requeues_blocked_turn_recovery_for_same_turn() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_resume_blocked";
        let thread_id = "thr_resume_blocked";
        let turn_id = "turn_resume_blocked";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_resume_blocked",
            None,
        )
        .await;
        crud_store
            .update_turn_status(
                thread_id,
                turn_id,
                TurnStatus::Blocked,
                Some("model unavailable"),
                1_700_000_010,
            )
            .await
            .expect("turn should be marked blocked");
        let job = crud_store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                "reasoning_resume_blocked".to_owned(),
                TurnItemType::Reasoning,
                None,
                RecoveryTrigger::ProviderError,
                RecoveryAction::BlockResumable,
                Some("model_not_found recovery has no fallback model".to_owned()),
                Some(ProviderFailureClass::ModelNotFound),
                Some(ProviderFailureStage::Finalize),
                None,
                1,
                0,
                serde_json::json!({}),
                serde_json::json!({
                    "base_backoff_secs": 0,
                    "max_wall_clock_secs": 10,
                    "no_progress_limit": 0,
                }),
                1_700_000_011,
            )
            .await
            .expect("blocked recovery job should enqueue");
        crud_store
            .mark_recovery_job_terminal(
                job.id.as_str(),
                RecoveryJobStatus::Blocked,
                Some("waiting for model config".to_owned()),
                1_700_000_012,
            )
            .await
            .expect("recovery job should be marked blocked");

        let resumed = coordinator
            .resume_blocked_turn(thread_id, turn_id, Some(job.id.as_str()), 1_700_000_020)
            .await
            .expect("blocked turn resume should succeed")
            .expect("blocked recovery job should resume");

        assert_eq!(resumed.id, job.id);
        assert_eq!(resumed.status, RecoveryJobStatus::Pending);
        assert_eq!(resumed.action, RecoveryAction::RestartTurn);
        assert!(resumed.max_attempts >= 1);
        let (_, turn) = crud_store
            .get_turn(thread_id, turn_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(turn.status, TurnStatus::InProgress);
        assert_eq!(turn.error, None);
    }

    #[tokio::test]
    async fn explicit_resume_requeues_blocked_tool_recovery_with_runtime_snapshot() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_resume_blocked_tool";
        let thread_id = "thr_resume_blocked_tool";
        let turn_id = "turn_resume_blocked_tool";
        let item_id = "tool_resume_blocked";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            item_id,
            None,
        )
        .await;
        persist_test_runtime_snapshot(crud_store.as_ref(), workspace_id, thread_id, turn_id).await;
        crud_store
            .update_turn_status(
                thread_id,
                turn_id,
                TurnStatus::Blocked,
                Some("tool recovery requires resume"),
                1_700_000_010,
            )
            .await
            .expect("turn should be marked blocked");
        let job = crud_store
            .enqueue_recovery_job(
                turn_id.to_owned(),
                item_id.to_owned(),
                TurnItemType::WebFetch,
                None,
                RecoveryTrigger::Timeout,
                RecoveryAction::BlockResumable,
                Some("tool recovery blocked".to_owned()),
                None,
                None,
                None,
                0,
                0,
                serde_json::json!({}),
                serde_json::json!({
                    "base_backoff_secs": 0,
                    "max_wall_clock_secs": 60,
                    "no_progress_limit": 3,
                }),
                1_700_000_011,
            )
            .await
            .expect("blocked tool recovery job should enqueue");
        crud_store
            .mark_recovery_job_terminal(
                job.id.as_str(),
                RecoveryJobStatus::Blocked,
                Some("waiting for operator resume".to_owned()),
                1_700_000_012,
            )
            .await
            .expect("recovery job should be marked blocked");

        let resumed = coordinator
            .resume_blocked_turn(thread_id, turn_id, Some(job.id.as_str()), 1_700_000_020)
            .await
            .expect("blocked tool turn resume should succeed")
            .expect("blocked tool recovery job should resume");

        assert_eq!(resumed.id, job.id);
        assert_eq!(resumed.status, RecoveryJobStatus::Pending);
        assert_eq!(resumed.action, RecoveryAction::RestartTurn);
        let (_, turn) = crud_store
            .get_turn(thread_id, turn_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(turn.status, TurnStatus::InProgress);
    }

    #[tokio::test]
    async fn due_pending_mark_failed_repair_terminalizes_without_claim() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: "turn_due_pending_mark_failed".to_owned(),
                    item_id: "reasoning_due_pending".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::InvalidRequest, "bad request"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();

        let pending = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.status, RecoveryJobStatus::Pending);
        assert!(pending.claim_token.is_none());

        let events = coordinator
            .repair_due_terminal_recovery_jobs(1_700_000_001, 64)
            .await
            .expect("terminal repair should run");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RecoveryExhausted(outcome)]
                if outcome.job_id == job.id
                    && outcome.status == RecoveryJobStatus::Failed
                    && outcome
                        .error_message
                        .contains("recovery policy marks this failure as terminal")
        ));
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Failed);
        assert_eq!(reloaded.run_count, 0);
        assert!(reloaded.claim_token.is_none());
    }
}
