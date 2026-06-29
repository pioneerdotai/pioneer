use anyhow::{Result, bail};
use pioneer_agent::{
    AgentControlError, AgentManager, ExecutionCheckpointContext, RecoveryAttemptRequest,
    RestoredRecoveryTurnRequest, RetainedToolLlmContext,
};
use pioneer_config::GatewayCommandExecutionTimeoutConfig;
use pioneer_crud::{
    BlockedTurnRecoveryResumeOutcome, ClaimedRecoveryActivation, CrudStore, RecoveryJobRecord,
    TimeoutCandidate,
};
use pioneer_protocol::{
    ExecutionCheckpointPayload, ExecutionWindowStatus, ProviderFailureClass,
    ProviderFailureDetails, RecoveryAction, RecoveryAttemptContext, RecoveryJobStatus,
    RecoveryTrigger, ToolMetadata, ToolRecoveryIdempotencyMode, ToolRecoveryPolicySnapshot,
    ToolRecoveryRetryClass, TurnItem, TurnItemType, TurnStatus, generate_id,
};
use pioneer_provider::ProviderRegistry;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

const RECOVERY_JOB_CLAIM_LEASE_SECS: u64 = 45;
const ACTIVE_RECOVERY_RECHECK_SECS: i64 = 2;
const RECOVERY_ATTEMPT_ID_LEN: usize = 21;
const STREAM_TO_NON_STREAM_FALLBACK_ATTEMPT: u32 = 2;

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
                max_wall_clock_secs: 180,
                no_progress_limit: 3,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::RateLimit,
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 4,
                base_backoff_secs: 3,
                max_wall_clock_secs: 300,
                no_progress_limit: 4,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::Provider5xx,
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 3,
                base_backoff_secs: 2,
                max_wall_clock_secs: 180,
                no_progress_limit: 3,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::AuthExpired,
            ProviderRecoveryPolicy {
                action: RefreshProviderAuth,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
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
                max_attempts: 2,
                base_backoff_secs: 1,
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::ContextTooLarge,
            ProviderRecoveryPolicy {
                action: CompactHistory,
                max_attempts: 2,
                base_backoff_secs: 1,
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::UnsupportedStreaming,
            ProviderRecoveryPolicy {
                action: DisableStreaming,
                max_attempts: 2,
                base_backoff_secs: 1,
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::UnsupportedParameter,
            ProviderRecoveryPolicy {
                action: AdaptProviderRequest,
                max_attempts: 2,
                base_backoff_secs: 1,
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::UnsupportedCapability,
            ProviderRecoveryPolicy {
                action: DisableUnsupportedCapability,
                max_attempts: 1,
                base_backoff_secs: 1,
                max_wall_clock_secs: 60,
                no_progress_limit: 1,
            },
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
            ProviderRecoveryPolicy {
                action: AdaptProviderRequest,
                max_attempts: 1,
                base_backoff_secs: 1,
                max_wall_clock_secs: 60,
                no_progress_limit: 1,
            },
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
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::InvalidRequest,
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
            },
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
            ProviderRecoveryPolicy {
                action: RetryWithBackoff,
                max_attempts: 1,
                base_backoff_secs: 2,
                max_wall_clock_secs: 60,
                no_progress_limit: 1,
            },
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
            .unwrap_or(ProviderRecoveryPolicy {
                action: RecoveryAction::RetryWithBackoff,
                max_attempts: 1,
                base_backoff_secs: 2,
                max_wall_clock_secs: 60,
                no_progress_limit: 1,
            })
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
    SnapshotMismatch,
    SnapshotInvalid { error: String },
}

impl RestoredRecoveryTurnUnavailable {
    fn lost_loop_block_reason(&self) -> Option<String> {
        match self {
            Self::MissingRuntimeSnapshot => Some(
                "cannot restore recovery after agent loop loss because durable turn runtime snapshot is missing"
                    .to_owned(),
            ),
            Self::SnapshotMismatch => Some(
                "cannot restore recovery after agent loop loss because durable turn runtime snapshot does not match the turn"
                    .to_owned(),
            ),
            Self::SnapshotInvalid { error } => Some(format!(
                "cannot restore recovery after agent loop loss because durable turn runtime snapshot is invalid: {error}"
            )),
            Self::TurnNotFound | Self::TurnNotInProgress => None,
        }
    }
}

impl RecoveryCoordinator {
    pub fn new(
        crud_store: Arc<CrudStore>,
        agent_manager: Arc<AgentManager>,
        _provider_registry: Arc<ProviderRegistry>,
        policy_registry: RecoveryPolicyRegistry,
    ) -> Self {
        Self {
            crud_store,
            agent_manager,
            policy_registry,
        }
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

        let provider_policy = self
            .policy_registry
            .policy_for_provider_failure(candidate.failure.class);

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

        self.record_active_recovery_failure(
            job,
            recovery_attempt_id,
            failure.message,
            failure.retry_after_ms,
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
            if wall_clock_elapsed <= max_wall_clock_secs {
                continue;
            }

            let active_elapsed = job
                .active_attempt_started_at_unix
                .map(|started_at| now_unix.saturating_sub(started_at))
                .unwrap_or_else(|| now_unix.saturating_sub(job.updated_at_unix));
            let message = Some(format!(
                "active recovery attempt exceeded wall-clock budget after {wall_clock_elapsed}s (active for {active_elapsed}s)"
            ));

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

        let retained_llm_context = self
            .retained_llm_context_for_turn(job.turn_id.as_str())
            .await?;
        let execution_checkpoint_context = self
            .execution_checkpoint_context_for_turn(thread_id.as_str(), job.turn_id.as_str())
            .await?;
        let continue_generation =
            execution_plan.continue_generation || execution_checkpoint_context.is_some();

        let request = RecoveryAttemptRequest {
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
            retained_llm_context,
            execution_checkpoint_context,
        };

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
                                    .block_recovery_without_restorable_runtime_snapshot(
                                        job,
                                        active_attempt_id,
                                        reason,
                                        now_unix,
                                    )
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

    async fn block_recovery_without_restorable_runtime_snapshot(
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

        Ok(Some(ExecutionCheckpointContext {
            window_id: window.id,
            window_index: window.window_index,
            checkpoint_id: checkpoint.id,
            checkpoint_kind: checkpoint_kind_label(checkpoint.checkpoint_kind),
            payload,
        }))
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

        let mut request =
            match crate::turn_runtime_snapshot::restored_recovery_turn_request_from_snapshot(
                &snapshot,
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
        let execution_window_index = self
            .next_restored_execution_window_index(turn_id, now_unix)
            .await?;
        request.execution_window_index = execution_window_index;
        Ok(RestoredRecoveryTurnRequestLookup::Available(request))
    }

    async fn next_restored_execution_window_index(
        &self,
        turn_id: &str,
        now_unix: i64,
    ) -> Result<u32> {
        let Some(window) = self
            .crud_store
            .latest_turn_execution_window(turn_id)
            .await?
        else {
            return Ok(1);
        };

        if window.status == ExecutionWindowStatus::Running {
            let counts = self
                .crud_store
                .count_turn_execution_window_terminal_items(turn_id)
                .await?;
            let completed_at = chrono::DateTime::<chrono::Utc>::from_timestamp(now_unix, 0)
                .map(|timestamp| timestamp.fixed_offset())
                .unwrap_or_else(|| chrono::Utc::now().fixed_offset());
            let mut metadata_json = window.metadata_json.clone();
            match metadata_json.as_object_mut() {
                Some(metadata) => {
                    metadata.insert(
                        "interruptedBy".to_owned(),
                        serde_json::Value::String("startup_recovery".to_owned()),
                    );
                    metadata.insert(
                        "terminalReason".to_owned(),
                        serde_json::Value::String(
                            "agent loop was restored after process loss".to_owned(),
                        ),
                    );
                }
                None => {
                    metadata_json = serde_json::json!({
                        "interruptedBy": "startup_recovery",
                        "terminalReason": "agent loop was restored after process loss",
                    });
                }
            }
            self.crud_store
                .mark_turn_execution_window_interrupted(
                    window.id.as_str(),
                    pioneer_crud::TurnExecutionWindowStatsRecord {
                        agent_round_count: window.agent_round_count.max(counts.agent_round_count),
                        tool_call_count: window.tool_call_count.max(counts.tool_call_count),
                        provider_token_count: window.provider_token_count,
                        metadata_json,
                        completed_at,
                        updated_at: completed_at,
                    },
                )
                .await?;
        }

        Ok(window.window_index.saturating_add(1).max(1))
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
            return Ok(TimeoutRecoveryPolicyDecision {
                policy: policy_from_tool_snapshot(snapshot),
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

    async fn retained_llm_context_for_turn(
        &self,
        turn_id: &str,
    ) -> Result<Vec<RetainedToolLlmContext>> {
        let rows = self.crud_store.list_turn_llm_context(turn_id).await?;
        let mut retained = Vec::with_capacity(rows.len());

        for row in rows {
            let Some(item_id) = row.item_id else {
                continue;
            };
            let Some(tool_name) = row.tool_name else {
                continue;
            };
            let payload = serde_json::from_str::<serde_json::Value>(row.payload.as_str())?;
            let Some(item) = self
                .crud_store
                .get_turn_item(turn_id, item_id.as_str())
                .await?
            else {
                bail!(
                    "retained llm_view exists for turn `{turn_id}` item `{item_id}`, but persisted turn item is missing"
                );
            };

            retained.push(RetainedToolLlmContext {
                item_id,
                tool_name,
                arguments: turn_item_arguments_json(&item),
                sequence: row.sequence,
                payload,
            });
        }

        retained.sort_by_key(|entry| entry.sequence);
        Ok(retained)
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
                plan.force_non_stream = true;
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
            ProviderFailureClass::UnsupportedParameter
            | ProviderFailureClass::MalformedProviderRequest
            | ProviderFailureClass::ProviderRejected
            | ProviderFailureClass::InvalidRequest => {
                if attempt_number >= STREAM_TO_NON_STREAM_FALLBACK_ATTEMPT
                    && provider_snapshot_field(job, "transport") == Some("stream")
                {
                    plan.force_non_stream = true;
                }
            }
            ProviderFailureClass::UnsupportedCapability
            | ProviderFailureClass::UnsupportedToolCalling => {
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
                if attempt_number >= STREAM_TO_NON_STREAM_FALLBACK_ATTEMPT
                    && provider_snapshot_field(job, "transport") == Some("stream")
                {
                    plan.force_non_stream = true;
                }
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

fn turn_item_arguments_json(item: &TurnItem) -> String {
    let arguments = match item {
        TurnItem::CommandExecution { arguments, .. }
        | TurnItem::FileChange { arguments, .. }
        | TurnItem::WebSearch { arguments, .. }
        | TurnItem::WebFetch { arguments, .. }
        | TurnItem::Download { arguments, .. }
        | TurnItem::DynamicToolCall { arguments, .. } => arguments,
        TurnItem::UserMessage { .. }
        | TurnItem::AgentMessage { .. }
        | TurnItem::Reasoning { .. }
        | TurnItem::SystemEvent { .. }
        | TurnItem::Task { .. } => return "{}".to_owned(),
    };
    serde_json::to_string(arguments).unwrap_or_else(|_| "{}".to_owned())
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

fn provider_failure_class_name(class: ProviderFailureClass) -> &'static str {
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
        RecoveryJobEnqueueOutcome, RecoveryPolicyRegistry,
    };
    use migration::{Migrator, MigratorTrait};
    use pioneer_agent::{
        AgentManager, AgentTurnHookRuntimeContext, ResolvedArtifactInput,
        SkillsDependenciesLoopConfig, SkillsLoopConfig, SkillsRuntimeLoopConfig,
        SkillsSecurityLoopConfig, SkillsValidationLoopConfig, ToolLoopConfig, WorkspaceSkillPolicy,
    };
    use pioneer_crud::{
        ClaimedRecoveryActivation, CrudStore, NewTurnExecutionCheckpointRecord,
        NewTurnExecutionWindowRecord, NewTurnRuntimeSnapshot, RecoveryJobRecord, TimeoutCandidate,
        TurnExecutionCheckpointKind, TurnExecutionWindowStatsRecord,
    };
    use pioneer_protocol::{
        ExecutionWindowExhaustionReason, ExecutionWindowStatus, ItemStartedNotification,
        ProviderFailureClass, ProviderFailureDetails, ProviderFailureStage, ProviderTransportKind,
        RecoveryAction, RecoveryAttemptContext, RecoveryJobStatus, RecoveryTrigger, SandboxMode,
        Thread, ThreadMode, ThreadOriginKind, ThreadSidebarVisibility, ThreadStatus,
        ToolCallStatus, ToolDisplayPayload, ToolOutputPolicySnapshot, ToolRecoveryIdempotencyMode,
        ToolRecoveryPolicySnapshot, ToolRecoveryRetryClass, ToolStoragePayload, TurnCapability,
        TurnCapabilityKind, TurnCompletedNotification, TurnItem, TurnItemTimeoutReason,
        TurnItemType, TurnStatus, UserInput, build_execution_checkpoint_payload,
    };
    use pioneer_provider::{
        ChatMessage, InputContentType, MessageAttachment, ProviderRegistry, providers::EchoProvider,
    };
    use pioneer_skills::{SkillPolicyKey, SkillTrustLevel};
    use pioneer_tools::{
        ComputerUseToolsConfig, ExecutionWindowsConfig, ToolLoopBudgetConfig,
        ToolRetryBudgetConfig, WebToolsConfig,
    };
    use sea_orm::Database;
    use std::collections::HashMap;
    use std::sync::Arc;

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
                user_roots: Vec::new(),
                registry_roots: Vec::new(),
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

    async fn setup_coordinator_with_agent()
    -> (Arc<CrudStore>, Arc<AgentManager>, RecoveryCoordinator) {
        let connection = Database::connect("sqlite::memory:")
            .await
            .expect("must connect sqlite memory");
        Migrator::up(&connection, None)
            .await
            .expect("migrations must succeed");
        let crud_store = Arc::new(CrudStore::new(connection));
        let provider_registry = Arc::new(ProviderRegistry::with_provider(
            "echo",
            Arc::new(EchoProvider::new()),
        ));
        let agent_manager = Arc::new(AgentManager::new(
            provider_registry.clone(),
            test_tool_loop_config(),
        ));
        let coordinator = RecoveryCoordinator::new(
            crud_store.clone(),
            agent_manager.clone(),
            provider_registry,
            RecoveryPolicyRegistry::default(),
        );
        (crud_store, agent_manager, coordinator)
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
            turns: Vec::new(),
        };
        let turn = pioneer_protocol::Turn {
            id: turn_id.to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: None,
        };

        crud_store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &[UserInput::Text {
                    text: "run tool".to_owned(),
                    text_elements: Vec::new(),
                }],
            )
            .await
            .expect("turn start should persist");
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
        let mut workspace_skill_policies = HashMap::new();
        workspace_skill_policies.insert(
            SkillPolicyKey::new("writer", "user"),
            WorkspaceSkillPolicy {
                enabled: Some(true),
                allow_implicit_invocation: Some(false),
            },
        );
        let capabilities = vec![TurnCapability {
            id: "cap_writer".to_owned(),
            kind: TurnCapabilityKind::Skill {
                slug: "writer".to_owned(),
                source_kind: "user".to_owned(),
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
        )
        .expect("runtime snapshot should serialize");
        crud_store
            .upsert_turn_runtime_snapshot(snapshot)
            .await
            .expect("runtime snapshot should persist");
    }

    #[tokio::test]
    async fn restored_recovery_turn_request_uses_runtime_snapshot() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let workspace_id = "ws_restore_snapshot";
        let thread_id = "thr_restore_snapshot";
        let turn_id = "turn_restore_snapshot";
        materialize_turn_with_tool_item(
            crud_store.as_ref(),
            workspace_id,
            thread_id,
            turn_id,
            "tool_restore_snapshot",
            None,
        )
        .await;
        persist_test_runtime_snapshot(crud_store.as_ref(), workspace_id, thread_id, turn_id).await;

        let restored = coordinator
            .restored_recovery_turn_request(thread_id, turn_id, 1_700_000_000)
            .await
            .expect("restored request should load")
            .into_available()
            .expect("runtime snapshot should be restorable");

        assert_eq!(restored.turn_id, turn_id);
        assert_eq!(restored.execution_window_index, 1);
        assert_eq!(restored.provider_name, "echo");
        assert_eq!(
            restored.hook_runtime_context.task_id.as_deref(),
            Some("task_snapshot")
        );
        assert_eq!(restored.workspace_skill_policies.len(), 1);
        assert_eq!(restored.capabilities.len(), 1);
        assert_eq!(restored.resolved_artifacts.len(), 1);
        assert_eq!(
            restored
                .runtime_environment
                .get("PIONEER_ARTIFACT_OUTPUT_DIR")
                .map(String::as_str),
            Some("/tmp/pioneer-snapshot-output")
        );
        assert_eq!(restored.history.len(), 2);
    }

    #[tokio::test]
    async fn restored_recovery_turn_request_closes_stale_running_window_and_advances_index() {
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

        let interrupted = crud_store
            .get_turn_execution_window(window.id.as_str())
            .await
            .expect("window read should succeed")
            .expect("window should exist");
        assert_eq!(interrupted.status, ExecutionWindowStatus::Interrupted);
        assert_eq!(
            interrupted
                .metadata_json
                .get("interruptedBy")
                .and_then(serde_json::Value::as_str),
            Some("startup_recovery")
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
        assert_eq!(context.window_id, first_window.id);
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
                &TimeoutCandidate {
                    attempt_id: "attempt_timeout_inside_recovery".to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: "recovery_reasoning".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    attempt_number: 1,
                    timeout_reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
                },
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
                &TimeoutCandidate {
                    attempt_id: "attempt_timeout_reuse".to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: "reasoning_2".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    attempt_number: 1,
                    timeout_reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
                },
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
                &TimeoutCandidate {
                    attempt_id: "attempt_tool_policy_snapshot".to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::WebFetch,
                    attempt_number: 1,
                    timeout_reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
                },
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
                &TimeoutCandidate {
                    attempt_id: "attempt_tool_missing_policy".to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: item_id.to_owned(),
                    item_type: TurnItemType::WebFetch,
                    attempt_number: 1,
                    timeout_reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
                },
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
            turns: Vec::new(),
        };
        let turn = pioneer_protocol::Turn {
            id: "turn_terminal_backfill".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: Default::default(),
            origin: Default::default(),
            error: None,
            prompt_manifest: None,
            permission_profile: None,
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
                        error: None,
                        prompt_manifest: None,
                        permission_profile: None,
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
                &TimeoutCandidate {
                    attempt_id: "attempt_timeout_before_late_provider".to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: "recovery_reasoning".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    attempt_number: 1,
                    timeout_reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
                },
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
    async fn active_recovery_watchdog_exhausts_stale_attempt_after_wall_clock_budget() {
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

        let events = coordinator
            .run_ready_jobs(1_700_000_181, 64)
            .await
            .expect("active watchdog should expire stale recovery");

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

        let late_events = coordinator
            .record_recovery_provider_failure(
                job.id.as_str(),
                active_attempt_id.as_str(),
                provider_failure(
                    ProviderFailureClass::NetworkTransient,
                    "late provider failure after watchdog",
                ),
                1_700_000_182,
            )
            .await
            .expect("late stale provider failure should not error");
        assert!(late_events.is_empty());
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
            .run_ready_jobs(1_700_000_181, 64)
            .await
            .expect("active watchdog should expire stale recovery");

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
                &TimeoutCandidate {
                    attempt_id: "attempt_tool_timeout".to_owned(),
                    turn_id: turn_id.to_owned(),
                    item_id: "tool_1".to_owned(),
                    item_type: TurnItemType::DynamicToolCall,
                    attempt_number: 1,
                    timeout_reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
                },
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
    async fn active_recovery_uses_persisted_policy_snapshot() {
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
                Some(ProviderFailureClass::InvalidRequest),
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
                provider_failure(ProviderFailureClass::InvalidRequest, "bad request"),
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
    async fn invalid_request_provider_policy_retries_instead_of_marking_failed() {
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
        assert_eq!(job.action, RecoveryAction::RetryWithBackoff);
        assert_eq!(job.max_attempts, 2);

        let events = coordinator
            .run_ready_jobs(1_700_000_001, 1)
            .await
            .expect("invalid-request recovery job should run");

        assert!(matches!(
            events.as_slice(),
            [RecoveryCoordinatorEvent::RetryScheduled { job_id, .. }]
                if job_id == &job.id
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
    async fn provider_rejected_policy_retries_instead_of_marking_failed() {
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
            .expect("provider rejection should enqueue retry recovery")
            .into_job();

        assert_eq!(job.action, RecoveryAction::RetryWithBackoff);
        assert_eq!(job.max_attempts, 2);
        assert_eq!(job.status, RecoveryJobStatus::Pending);
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
    async fn due_pending_mark_failed_repair_fails_without_claim() {
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

        assert!(events.is_empty());
        let reloaded = crud_store
            .get_recovery_job(job.id.as_str())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.status, RecoveryJobStatus::Pending);
        assert_eq!(reloaded.run_count, 0);
        assert!(reloaded.claim_token.is_none());
    }
}
