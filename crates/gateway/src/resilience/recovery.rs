use anyhow::{Result, bail};
use pioneer_agent::{
    AgentControlError, AgentManager, RecoveryAttemptRequest, RetainedToolLlmContext,
};
use pioneer_crud::{ClaimedRecoveryActivation, CrudStore, RecoveryJobRecord, TimeoutCandidate};
use pioneer_protocol::{
    ProviderFailureClass, ProviderFailureDetails, RecoveryAction, RecoveryAttemptContext,
    RecoveryJobStatus, RecoveryTrigger, ToolRecoveryIdempotencyMode, ToolRecoveryPolicySnapshot,
    ToolRecoveryRetryClass, TurnItem, TurnItemType, TurnStatus, generate_id,
};
use pioneer_provider::ProviderRegistry;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{Duration, timeout};

const RECOVERY_JOB_CLAIM_LEASE_SECS: u64 = 45;
const ACTIVE_RECOVERY_RECHECK_SECS: i64 = 2;
const RECOVERY_ATTEMPT_ID_LEN: usize = 21;
const MODEL_FALLBACK_LOOKUP_TIMEOUT: Duration = Duration::from_secs(8);
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
                max_wall_clock_secs: 300,
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
                action: RetryWithBackoff,
                max_attempts: 2,
                base_backoff_secs: 2,
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::ModelNotFound,
            ProviderRecoveryPolicy {
                action: Fallback,
                max_attempts: 1,
                base_backoff_secs: 1,
                max_wall_clock_secs: 30,
                no_progress_limit: 1,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::PromptTooLong,
            ProviderRecoveryPolicy {
                action: Fallback,
                max_attempts: 1,
                base_backoff_secs: 1,
                max_wall_clock_secs: 45,
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
                max_wall_clock_secs: 120,
                no_progress_limit: 2,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::StreamTruncated,
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
                action: MarkFailed,
                max_attempts: 0,
                base_backoff_secs: 0,
                max_wall_clock_secs: 30,
                no_progress_limit: 0,
            },
        );
        by_provider_failure_class.insert(
            ProviderFailureClass::PermissionDenied,
            ProviderRecoveryPolicy {
                action: MarkFailed,
                max_attempts: 0,
                base_backoff_secs: 0,
                max_wall_clock_secs: 30,
                no_progress_limit: 0,
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

#[derive(Clone)]
pub struct RecoveryCoordinator {
    crud_store: Arc<CrudStore>,
    agent_manager: Arc<AgentManager>,
    provider_registry: Arc<ProviderRegistry>,
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
    RecoveryExhausted(RecoveryTerminalOutcome),
}

#[derive(Debug, Default, Clone)]
struct RecoveryAttemptPlan {
    force_non_stream: bool,
    refresh_provider_auth: bool,
    compact_history: bool,
    continue_generation: bool,
    model_override: Option<String>,
    terminal_reason: Option<String>,
}

impl RecoveryCoordinator {
    pub fn new(
        crud_store: Arc<CrudStore>,
        agent_manager: Arc<AgentManager>,
        provider_registry: Arc<ProviderRegistry>,
        policy_registry: RecoveryPolicyRegistry,
    ) -> Self {
        Self {
            crud_store,
            agent_manager,
            provider_registry,
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
                    Some("turn failed; pending recovery jobs cancelled".to_owned()),
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
        let mut events = self.backfill_timeout_jobs(now_unix, limit).await?;
        events.extend(self.expire_active_recovery_jobs(now_unix, limit).await?);

        let jobs = self
            .crud_store
            .claim_due_recovery_jobs(now_unix, RECOVERY_JOB_CLAIM_LEASE_SECS, limit)
            .await?;

        for job in jobs {
            events.extend(self.run_single_job(job, now_unix).await?);
        }
        Ok(events)
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
            let message = "recovery policy marks this failure as terminal".to_owned();
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

        let Some((thread_id, _workspace_id)) = self
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

        let request = RecoveryAttemptRequest {
            recovery_job_id: job.id.clone(),
            recovery_attempt_id: active_attempt_id.clone(),
            turn_id: job.turn_id.clone(),
            item_id: job.item_id.clone(),
            item_type: job.item_type,
            force_non_stream: execution_plan.force_non_stream,
            refresh_provider_auth: execution_plan.refresh_provider_auth,
            compact_history: execution_plan.compact_history,
            continue_generation: execution_plan.continue_generation,
            model_override: execution_plan.model_override,
            retained_llm_context,
        };

        match self
            .agent_manager
            .start_recovery_attempt(thread_id.as_str(), request)
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
                let terminal = matches!(
                    error,
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
            }
        }

        Ok(events)
    }
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
            | ProviderFailureClass::Provider5xx => {}
            ProviderFailureClass::StreamStall | ProviderFailureClass::StreamTruncated => {
                if attempt_number >= STREAM_TO_NON_STREAM_FALLBACK_ATTEMPT {
                    plan.force_non_stream = true;
                }
            }
            ProviderFailureClass::AuthExpired => {
                plan.refresh_provider_auth = true;
            }
            ProviderFailureClass::ModelNotFound => {
                plan.model_override = self.select_fallback_model(job).await?;
                if plan.model_override.is_none() {
                    plan.terminal_reason =
                        Some("model_not_found recovery has no fallback model".to_owned());
                }
            }
            ProviderFailureClass::PromptTooLong => {
                plan.compact_history = true;
            }
            ProviderFailureClass::MaxOutputTokens => {
                plan.continue_generation = true;
            }
            ProviderFailureClass::InvalidRequest | ProviderFailureClass::PermissionDenied => {}
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

        // TODO(fallback_model): replace "first different model" heuristic with ranked selection
        // (compatibility class, capabilities, and provider preferences).
        let Some(provider_name) = provider_snapshot_field(job, "provider") else {
            return Ok(None);
        };
        let Some(failed_model) = provider_snapshot_field(job, "model") else {
            return Ok(None);
        };
        let provider = match self.provider_registry.get_or_create(provider_name) {
            Ok(provider) => provider,
            Err(_) => return Ok(None),
        };

        let models = match timeout(MODEL_FALLBACK_LOOKUP_TIMEOUT, provider.list_models()).await {
            Ok(Ok(models)) => models,
            _ => return Ok(None),
        };

        Ok(models
            .into_iter()
            .map(|model| model.id)
            .find(|candidate| !candidate.trim().is_empty() && candidate != failed_model))
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
    base.action = RecoveryAction::MarkFailed;
    base.max_attempts = 1;
    base.base_backoff_secs = 1;
    base.no_progress_limit = 1;
    base
}

fn policy_action_name(action: RecoveryAction) -> &'static str {
    match action {
        RecoveryAction::RetryAttempt => "retry_attempt",
        RecoveryAction::RetryWithBackoff => "retry_with_backoff",
        RecoveryAction::RestartTurn => "restart_turn",
        RecoveryAction::Fallback => "fallback",
        RecoveryAction::MarkFailed => "mark_failed",
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
        AgentManager, SkillsDependenciesLoopConfig, SkillsLoopConfig, SkillsRuntimeLoopConfig,
        SkillsSecurityLoopConfig, SkillsValidationLoopConfig, ToolLoopConfig,
    };
    use pioneer_crud::{ClaimedRecoveryActivation, CrudStore, TimeoutCandidate};
    use pioneer_protocol::{
        ItemStartedNotification, ProviderFailureClass, ProviderFailureDetails,
        ProviderFailureStage, ProviderTransportKind, RecoveryAction, RecoveryAttemptContext,
        RecoveryJobStatus, RecoveryTrigger, SandboxMode, Thread, ThreadMode, ThreadOriginKind,
        ThreadSidebarVisibility, ThreadStatus, ToolCallStatus, ToolDisplayPayload,
        ToolOutputPolicySnapshot, ToolRecoveryIdempotencyMode, ToolRecoveryPolicySnapshot,
        ToolRecoveryRetryClass, ToolStoragePayload, TurnCompletedNotification, TurnItem,
        TurnItemTimeoutReason, TurnItemType, TurnStatus, UserInput,
    };
    use pioneer_provider::{ProviderRegistry, providers::EchoProvider};
    use pioneer_skills::SkillTrustLevel;
    use pioneer_tools::{
        ComputerUseToolsConfig, ToolLoopBudgetConfig, ToolRetryBudgetConfig, WebToolsConfig,
    };
    use sea_orm::Database;
    use std::sync::Arc;

    fn test_tool_loop_config() -> ToolLoopConfig {
        ToolLoopConfig {
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
                    read_skill_max_chars: 24_000,
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
            retry: ToolRetryBudgetConfig::default(),
        }
    }

    async fn setup_coordinator() -> (Arc<CrudStore>, RecoveryCoordinator) {
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
            agent_manager,
            provider_registry,
            RecoveryPolicyRegistry::default(),
        );
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
            error: None,
            prompt_manifest: None,
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

        assert_eq!(job.action, RecoveryAction::MarkFailed);
        assert_eq!(job.max_attempts, 1);
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
            Some(1)
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
            error: None,
            prompt_manifest: None,
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
                        error: None,
                        prompt_manifest: None,
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
    async fn mark_failed_provider_policy_fails_without_recovery_attempt() {
        let (crud_store, coordinator) = setup_coordinator().await;
        let job = coordinator
            .enqueue_provider_failure_job(
                &ProviderFailureCandidate {
                    turn_id: "turn_mark_failed_recovery_job".to_owned(),
                    item_id: "reasoning_1".to_owned(),
                    item_type: TurnItemType::Reasoning,
                    failure: provider_failure(ProviderFailureClass::InvalidRequest, "bad request"),
                },
                1_700_000_000,
            )
            .await
            .expect("initial provider failure should enqueue")
            .into_job();

        let events = coordinator
            .run_ready_jobs(1_700_000_001, 1)
            .await
            .expect("mark-failed recovery job should run");

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
}
