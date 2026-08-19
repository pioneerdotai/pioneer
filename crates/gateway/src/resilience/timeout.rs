use anyhow::Result;
use pioneer_config::{
    GatewayCommandExecutionTimeoutConfig, GatewayContextCompactionTimeoutConfig,
    GatewayProviderStreamItemTimeoutConfig,
};
use pioneer_crud::{CrudStore, TimeoutCandidate, TurnItemAttemptDeadlines, TurnLivenessRecord};
use pioneer_protocol::{TurnItem, TurnItemExecutionClass, TurnItemTimeoutReason, TurnItemType};
use pioneer_provider::ProviderTimeoutPolicy;
use std::collections::HashMap;
use std::sync::Arc;

pub const TIMEOUT_RECOVERY_SUPPRESSED_TURN_PROGRESS: &str = "turn_progressed_after_item_frontier";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutPolicy {
    pub lease_secs: u64,
    pub idle_secs: u64,
    pub hard_secs: u64,
}

/// Defines whether confirmed runtime activity moves an attempt's hard
/// deadline. Long-running commands use a rolling inactivity horizon; bounded
/// internal operations such as context compaction retain an absolute cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HardDeadlineMode {
    Absolute,
    RollingOnConfirmedActivity,
}

/// Authoritative runtime evidence available when a running item reaches a
/// timeout boundary. `Unavailable` is deliberately distinct from terminal:
/// absence of observation is not evidence that execution stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeTimeoutObservation {
    NotApplicable,
    Active,
    Terminal,
    Unavailable,
}

pub const fn hard_deadline_mode(
    execution_class: TurnItemExecutionClass,
    item_type: TurnItemType,
) -> HardDeadlineMode {
    match (execution_class, item_type) {
        (TurnItemExecutionClass::Standard, TurnItemType::CommandExecution) => {
            HardDeadlineMode::RollingOnConfirmedActivity
        }
        _ => HardDeadlineMode::Absolute,
    }
}

pub const fn timeout_requires_runtime_evidence(
    reason: TurnItemTimeoutReason,
    execution_class: TurnItemExecutionClass,
    item_type: TurnItemType,
) -> bool {
    matches!(
        reason,
        TurnItemTimeoutReason::LeaseExpired | TurnItemTimeoutReason::IdleDeadlineExceeded
    ) || matches!(reason, TurnItemTimeoutReason::HardDeadlineExceeded)
        && matches!(
            hard_deadline_mode(execution_class, item_type),
            HardDeadlineMode::RollingOnConfirmedActivity
        )
}

impl TimeoutPolicy {
    pub fn deadlines(&self, now_unix: i64) -> (i64, i64, i64) {
        (
            saturating_add_secs(now_unix, self.lease_secs),
            saturating_add_secs(now_unix, self.idle_secs),
            saturating_add_secs(now_unix, self.hard_secs),
        )
    }
}

impl From<GatewayCommandExecutionTimeoutConfig> for TimeoutPolicy {
    fn from(config: GatewayCommandExecutionTimeoutConfig) -> Self {
        Self {
            lease_secs: config.lease_secs,
            idle_secs: config.idle_secs,
            hard_secs: config.hard_secs,
        }
    }
}

impl From<GatewayProviderStreamItemTimeoutConfig> for TimeoutPolicy {
    fn from(config: GatewayProviderStreamItemTimeoutConfig) -> Self {
        Self {
            lease_secs: config.lease_secs,
            idle_secs: config.idle_secs,
            hard_secs: config.hard_secs,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeoutPolicyRegistry {
    by_item_type: HashMap<TurnItemType, TimeoutPolicy>,
    context_compaction: TimeoutPolicy,
    context_compaction_recovery_grace_secs: u64,
}

impl Default for TimeoutPolicyRegistry {
    fn default() -> Self {
        use TurnItemType::*;
        let mut by_item_type = HashMap::new();
        by_item_type.insert(
            UserMessage,
            TimeoutPolicy {
                lease_secs: 30,
                idle_secs: 30,
                hard_secs: 60,
            },
        );
        by_item_type.insert(
            AgentMessage,
            TimeoutPolicy::from(GatewayProviderStreamItemTimeoutConfig::default()),
        );
        by_item_type.insert(
            Reasoning,
            TimeoutPolicy::from(GatewayProviderStreamItemTimeoutConfig::default()),
        );
        by_item_type.insert(
            SystemEvent,
            TimeoutPolicy {
                lease_secs: 30,
                idle_secs: 30,
                hard_secs: 120,
            },
        );
        by_item_type.insert(
            CommandExecution,
            TimeoutPolicy::from(GatewayCommandExecutionTimeoutConfig::default()),
        );
        by_item_type.insert(
            FileChange,
            TimeoutPolicy {
                lease_secs: 60,
                idle_secs: 45,
                hard_secs: 3 * 60,
            },
        );
        by_item_type.insert(
            WebSearch,
            TimeoutPolicy {
                lease_secs: 120,
                idle_secs: 60,
                hard_secs: 3 * 60,
            },
        );
        by_item_type.insert(
            WebFetch,
            TimeoutPolicy {
                lease_secs: 120,
                idle_secs: 75,
                hard_secs: 4 * 60,
            },
        );
        by_item_type.insert(
            Download,
            TimeoutPolicy {
                lease_secs: 180,
                idle_secs: 120,
                hard_secs: 20 * 60,
            },
        );
        by_item_type.insert(
            DynamicToolCall,
            TimeoutPolicy {
                lease_secs: 120,
                idle_secs: 90,
                hard_secs: 5 * 60,
            },
        );
        let context_compaction_config = GatewayContextCompactionTimeoutConfig::default();
        Self {
            by_item_type,
            context_compaction: TimeoutPolicy {
                lease_secs: context_compaction_config.lease_secs,
                idle_secs: context_compaction_config.idle_secs,
                hard_secs: context_compaction_config.hard_secs,
            },
            context_compaction_recovery_grace_secs: context_compaction_config.recovery_grace_secs,
        }
    }
}

impl TimeoutPolicyRegistry {
    #[cfg(test)]
    pub fn with_provider_timeout_policy(provider_policy: ProviderTimeoutPolicy) -> Self {
        Self::with_provider_and_command_execution_timeout_policy(
            provider_policy,
            GatewayCommandExecutionTimeoutConfig::default(),
            GatewayProviderStreamItemTimeoutConfig::default(),
        )
    }

    #[cfg(test)]
    pub fn with_provider_and_command_execution_timeout_policy(
        _provider_policy: ProviderTimeoutPolicy,
        command_execution_config: GatewayCommandExecutionTimeoutConfig,
        provider_stream_item_config: GatewayProviderStreamItemTimeoutConfig,
    ) -> Self {
        Self::with_configured_timeout_policies(
            _provider_policy,
            command_execution_config,
            provider_stream_item_config,
            GatewayContextCompactionTimeoutConfig::default(),
        )
    }

    pub fn with_configured_timeout_policies(
        _provider_policy: ProviderTimeoutPolicy,
        command_execution_config: GatewayCommandExecutionTimeoutConfig,
        provider_stream_item_config: GatewayProviderStreamItemTimeoutConfig,
        context_compaction_config: GatewayContextCompactionTimeoutConfig,
    ) -> Self {
        use TurnItemType::{AgentMessage, Reasoning};

        let mut registry = Self::default();
        registry.by_item_type.insert(
            TurnItemType::CommandExecution,
            TimeoutPolicy::from(command_execution_config),
        );
        let timeout_policy = TimeoutPolicy::from(provider_stream_item_config);

        registry.by_item_type.insert(AgentMessage, timeout_policy);
        registry.by_item_type.insert(Reasoning, timeout_policy);
        registry.context_compaction = TimeoutPolicy {
            lease_secs: context_compaction_config.lease_secs,
            idle_secs: context_compaction_config.idle_secs,
            hard_secs: context_compaction_config.hard_secs,
        };
        registry.context_compaction_recovery_grace_secs =
            context_compaction_config.recovery_grace_secs;
        registry
    }

    pub fn policy_for(&self, item_type: TurnItemType) -> TimeoutPolicy {
        self.by_item_type
            .get(&item_type)
            .copied()
            .unwrap_or(TimeoutPolicy {
                lease_secs: 40,
                idle_secs: 30,
                hard_secs: 120,
            })
    }

    pub fn policy_for_item(&self, item: &TurnItem) -> TimeoutPolicy {
        self.policy_for_execution_class(item.execution_class(), item.item_type())
    }

    pub fn policy_for_execution_class(
        &self,
        execution_class: TurnItemExecutionClass,
        item_type: TurnItemType,
    ) -> TimeoutPolicy {
        match execution_class {
            TurnItemExecutionClass::Standard => self.policy_for(item_type),
            TurnItemExecutionClass::ContextCompaction => self.context_compaction,
        }
    }

    pub fn recovery_grace_secs(&self, execution_class: TurnItemExecutionClass) -> Option<u64> {
        match execution_class {
            TurnItemExecutionClass::Standard => None,
            TurnItemExecutionClass::ContextCompaction => {
                Some(self.context_compaction_recovery_grace_secs)
            }
        }
    }
}

#[derive(Clone)]
pub struct TimeoutSupervisor {
    crud_store: Arc<CrudStore>,
    policy_registry: TimeoutPolicyRegistry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeoutRecoveryClassification {
    RecoverTurn,
    SuppressRecoveryBecauseTurnProgressed { liveness: TurnLivenessRecord },
}

pub fn classify_timeout_candidate_liveness(
    candidate: &TimeoutCandidate,
    liveness: Option<&TurnLivenessRecord>,
) -> TimeoutRecoveryClassification {
    let Some(liveness) = liveness else {
        return TimeoutRecoveryClassification::RecoverTurn;
    };

    let heartbeat_frontier = candidate
        .last_heartbeat_at_unix
        .unwrap_or(candidate.started_at_unix);
    if liveness.last_activity_at_unix > heartbeat_frontier {
        return TimeoutRecoveryClassification::SuppressRecoveryBecauseTurnProgressed {
            liveness: liveness.clone(),
        };
    }

    if candidate
        .last_heartbeat_at_unix
        .unwrap_or(candidate.started_at_unix)
        == candidate.started_at_unix
        && candidate
            .started_event_sequence
            .is_some_and(|sequence| liveness.last_activity_sequence > sequence)
    {
        return TimeoutRecoveryClassification::SuppressRecoveryBecauseTurnProgressed {
            liveness: liveness.clone(),
        };
    }

    TimeoutRecoveryClassification::RecoverTurn
}

pub fn timeout_recovery_suppression_context(
    candidate: &TimeoutCandidate,
    liveness: &TurnLivenessRecord,
) -> serde_json::Value {
    serde_json::json!({
        "source": "timeout_classifier",
        "reason": TIMEOUT_RECOVERY_SUPPRESSED_TURN_PROGRESS,
        "attempt_id": candidate.attempt_id.as_str(),
        "item_id": candidate.item_id.as_str(),
        "item_type": format!("{:?}", candidate.item_type),
        "attempt_number": candidate.attempt_number,
        "timeout_reason": format!("{:?}", candidate.timeout_reason),
        "started_at_unix": candidate.started_at_unix,
        "started_event_sequence": candidate.started_event_sequence,
        "last_heartbeat_at_unix": candidate.last_heartbeat_at_unix,
        "turn_liveness": {
            "turn_id": liveness.turn_id.as_str(),
            "thread_id": liveness.thread_id.as_str(),
            "last_activity_sequence": liveness.last_activity_sequence,
            "last_activity_kind": liveness.last_activity_kind.as_str(),
            "last_activity_item_id": liveness.last_activity_item_id.as_deref(),
            "last_activity_item_type": liveness.last_activity_item_type.as_deref(),
            "last_activity_at_unix": liveness.last_activity_at_unix,
        }
    })
}

impl TimeoutSupervisor {
    pub fn new(crud_store: Arc<CrudStore>, policy_registry: TimeoutPolicyRegistry) -> Self {
        Self {
            crud_store,
            policy_registry,
        }
    }

    pub fn deadlines_for_item(
        &self,
        item: &TurnItem,
        started_at_unix: i64,
    ) -> TurnItemAttemptDeadlines {
        let policy = self.policy_registry.policy_for_item(item);
        let (lease_expires_at, idle_deadline_at, hard_deadline_at) =
            policy.deadlines(started_at_unix);
        TurnItemAttemptDeadlines {
            lease_expires_at_unix: Some(lease_expires_at),
            idle_deadline_at_unix: Some(idle_deadline_at),
            hard_deadline_at_unix: Some(hard_deadline_at),
        }
    }

    pub async fn backfill_missing_deadlines(&self, limit: u64) -> Result<usize> {
        let candidates = self
            .crud_store
            .list_running_attempts_missing_deadlines(limit)
            .await?;
        let mut backfilled = 0usize;
        for candidate in candidates {
            let deadlines = self
                .deadlines_for_stored_item(
                    candidate.turn_id.as_str(),
                    candidate.item_id.as_str(),
                    candidate.item_type,
                    candidate.started_at_unix,
                )
                .await?;
            let configured = self
                .crud_store
                .configure_turn_item_attempt_deadlines(
                    candidate.turn_id.as_str(),
                    candidate.item_id.as_str(),
                    candidate.started_at_unix,
                    deadlines.lease_expires_at_unix,
                    deadlines.idle_deadline_at_unix,
                    deadlines.hard_deadline_at_unix,
                )
                .await?;
            if configured {
                backfilled = backfilled.saturating_add(1);
            }
        }
        Ok(backfilled)
    }

    pub async fn heartbeat_item_attempt(
        &self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        now_unix: i64,
    ) -> Result<()> {
        let stored_attempt = self
            .crud_store
            .get_running_turn_item_attempt(turn_id, item_id)
            .await?;
        let (policy, execution_class, stored_item_type) = match stored_attempt {
            Some(attempt) => (
                self.policy_registry
                    .policy_for_execution_class(attempt.execution_class, attempt.item_type),
                attempt.execution_class,
                attempt.item_type,
            ),
            None => (
                self.policy_registry.policy_for(item_type),
                TurnItemExecutionClass::Standard,
                item_type,
            ),
        };
        let (lease_expires_at, idle_deadline_at, hard_deadline_at) = policy.deadlines(now_unix);
        let next_hard_deadline_at = matches!(
            hard_deadline_mode(execution_class, stored_item_type),
            HardDeadlineMode::RollingOnConfirmedActivity
        )
        .then_some(hard_deadline_at);
        let _ = self
            .crud_store
            .heartbeat_turn_item_attempt(
                turn_id,
                item_id,
                item_type,
                now_unix,
                Some(lease_expires_at),
                Some(idle_deadline_at),
                next_hard_deadline_at,
            )
            .await?;
        Ok(())
    }

    pub async fn list_timeout_candidates(
        &self,
        now_unix: i64,
        limit: u64,
    ) -> Result<Vec<TimeoutCandidate>> {
        self.crud_store
            .list_timeout_candidates(now_unix, limit)
            .await
    }

    pub async fn transition_timeout_candidate(
        &self,
        candidate: &TimeoutCandidate,
        now_unix: i64,
    ) -> Result<bool> {
        self.crud_store
            .transition_timeout_candidate(candidate, now_unix)
            .await
    }

    pub async fn reactivate_timeout_source_attempt_after_runtime_activity(
        &self,
        recovery_job_id: &str,
        now_unix: i64,
    ) -> Result<bool> {
        let Some(job) = self.crud_store.get_recovery_job(recovery_job_id).await? else {
            return Ok(false);
        };
        let Some(source_attempt_id) = job.source_attempt_id.as_deref() else {
            return Ok(false);
        };
        let Some(candidate) = self
            .crud_store
            .get_timed_out_attempt_candidate(source_attempt_id)
            .await?
        else {
            return Ok(false);
        };
        if !matches!(
            hard_deadline_mode(candidate.execution_class, candidate.item_type),
            HardDeadlineMode::RollingOnConfirmedActivity
        ) {
            return Ok(false);
        }
        let policy = self
            .policy_registry
            .policy_for_execution_class(candidate.execution_class, candidate.item_type);
        let (lease_expires_at, idle_deadline_at, hard_deadline_at) = policy.deadlines(now_unix);
        self.crud_store
            .reactivate_timed_out_attempt(
                source_attempt_id,
                now_unix,
                TurnItemAttemptDeadlines {
                    lease_expires_at_unix: Some(lease_expires_at),
                    idle_deadline_at_unix: Some(idle_deadline_at),
                    hard_deadline_at_unix: Some(hard_deadline_at),
                },
            )
            .await
    }

    pub async fn classify_timeout_candidate(
        &self,
        candidate: &TimeoutCandidate,
    ) -> Result<TimeoutRecoveryClassification> {
        let liveness = self
            .crud_store
            .get_turn_liveness(candidate.turn_id.as_str())
            .await?;
        Ok(classify_timeout_candidate_liveness(
            candidate,
            liveness.as_ref(),
        ))
    }

    pub async fn renew_running_attempt_deadlines_for_turn(
        &self,
        turn_id: &str,
        now_unix: i64,
    ) -> Result<usize> {
        self.renew_running_attempt_deadlines_inner(turn_id, now_unix, false)
            .await
    }

    /// Renews an active attempt after an externally observed runtime event.
    ///
    /// Process-local observation is deliberately not accepted by the normal
    /// renewal path.  A CLI runtime observation or a user response is a
    /// causal event received at the gateway boundary, so it is first recorded
    /// durably and then may renew the attempt's configured activity horizons.
    /// Only command execution uses a rolling hard deadline; bounded internal
    /// operations retain their immutable absolute cap.
    pub async fn renew_running_attempt_deadlines_after_runtime_activity(
        &self,
        turn_id: &str,
        now_unix: i64,
        activity_kind: &str,
    ) -> Result<usize> {
        self.crud_store
            .observe_turn_runtime_activity(turn_id, activity_kind, now_unix)
            .await?;
        self.renew_running_attempt_deadlines_inner(turn_id, now_unix, true)
            .await
    }

    async fn renew_running_attempt_deadlines_inner(
        &self,
        turn_id: &str,
        now_unix: i64,
        allow_runtime_activity: bool,
    ) -> Result<usize> {
        let attempts = self
            .crud_store
            .list_running_turn_item_attempts_for_turn(turn_id)
            .await?;
        let Some(liveness) = self.crud_store.get_turn_liveness(turn_id).await? else {
            // Process-local `active_turn_id` is not causal progress.  Without
            // a durable event frontier there is nothing safe to renew.
            return Ok(0);
        };
        let mut renewed = 0usize;
        for attempt in attempts {
            // The supervisor's own observation heartbeat is deliberately not
            // accepted as evidence.  Only a new durable item/provider/tool
            // activity frontier can renew an idle lease.
            if !allow_runtime_activity
                && (liveness.last_activity_kind.starts_with("runtime/")
                    || liveness.last_activity_at_unix
                        <= attempt
                            .last_heartbeat_at_unix
                            .unwrap_or(attempt.started_at_unix))
            {
                continue;
            }

            let policy = self
                .policy_registry
                .policy_for_execution_class(attempt.execution_class, attempt.item_type);
            let (lease_expires_at, idle_deadline_at, hard_deadline_at) = policy.deadlines(now_unix);
            let next_hard_deadline_at = matches!(
                hard_deadline_mode(attempt.execution_class, attempt.item_type),
                HardDeadlineMode::RollingOnConfirmedActivity
            )
            .then_some(hard_deadline_at);
            let deadlines = TurnItemAttemptDeadlines {
                lease_expires_at_unix: Some(lease_expires_at),
                idle_deadline_at_unix: Some(idle_deadline_at),
                hard_deadline_at_unix: next_hard_deadline_at,
            };
            if self
                .crud_store
                .heartbeat_turn_item_attempt(
                    attempt.turn_id.as_str(),
                    attempt.item_id.as_str(),
                    attempt.item_type,
                    now_unix,
                    deadlines.lease_expires_at_unix,
                    deadlines.idle_deadline_at_unix,
                    deadlines.hard_deadline_at_unix,
                )
                .await?
            {
                renewed = renewed.saturating_add(1);
            }
        }
        Ok(renewed)
    }

    async fn deadlines_for_stored_item(
        &self,
        turn_id: &str,
        item_id: &str,
        fallback_item_type: TurnItemType,
        started_at_unix: i64,
    ) -> Result<TurnItemAttemptDeadlines> {
        let policy = match self
            .crud_store
            .get_running_turn_item_attempt(turn_id, item_id)
            .await?
        {
            Some(attempt) => self
                .policy_registry
                .policy_for_execution_class(attempt.execution_class, attempt.item_type),
            None => self.policy_registry.policy_for(fallback_item_type),
        };
        let (lease_expires_at, idle_deadline_at, hard_deadline_at) =
            policy.deadlines(started_at_unix);
        Ok(TurnItemAttemptDeadlines {
            lease_expires_at_unix: Some(lease_expires_at),
            idle_deadline_at_unix: Some(idle_deadline_at),
            hard_deadline_at_unix: Some(hard_deadline_at),
        })
    }

    pub fn recovery_grace_secs_for_candidate(&self, candidate: &TimeoutCandidate) -> Option<u64> {
        self.policy_registry
            .recovery_grace_secs(candidate.execution_class)
    }
}

fn saturating_add_secs(base: i64, seconds: u64) -> i64 {
    base.saturating_add(i64::try_from(seconds).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{SystemEventLevel, TurnItem, TurnItemTimeoutReason, TurnItemType};

    fn timeout_candidate() -> TimeoutCandidate {
        TimeoutCandidate {
            attempt_id: "attempt_1".to_owned(),
            turn_id: "turn_1".to_owned(),
            item_id: "reasoning_1".to_owned(),
            item_type: TurnItemType::Reasoning,
            execution_class: TurnItemExecutionClass::Standard,
            attempt_number: 1,
            timeout_reason: TurnItemTimeoutReason::IdleDeadlineExceeded,
            started_at_unix: 1_000,
            started_event_sequence: Some(4),
            last_heartbeat_at_unix: Some(1_000),
            lease_expires_at_unix: Some(1_900),
            idle_deadline_at_unix: Some(1_900),
            hard_deadline_at_unix: Some(2_800),
        }
    }

    #[test]
    fn command_hard_deadline_is_rolling_but_context_compaction_is_absolute() {
        assert_eq!(
            hard_deadline_mode(
                TurnItemExecutionClass::Standard,
                TurnItemType::CommandExecution,
            ),
            HardDeadlineMode::RollingOnConfirmedActivity
        );
        assert_eq!(
            hard_deadline_mode(
                TurnItemExecutionClass::ContextCompaction,
                TurnItemType::SystemEvent,
            ),
            HardDeadlineMode::Absolute
        );
        assert!(timeout_requires_runtime_evidence(
            TurnItemTimeoutReason::HardDeadlineExceeded,
            TurnItemExecutionClass::Standard,
            TurnItemType::CommandExecution,
        ));
        assert!(!timeout_requires_runtime_evidence(
            TurnItemTimeoutReason::HardDeadlineExceeded,
            TurnItemExecutionClass::ContextCompaction,
            TurnItemType::SystemEvent,
        ));
    }

    fn turn_liveness(at_unix: i64, sequence: i64) -> TurnLivenessRecord {
        TurnLivenessRecord {
            turn_id: "turn_1".to_owned(),
            thread_id: "thread_1".to_owned(),
            last_activity_sequence: sequence,
            last_activity_kind: "item/completed".to_owned(),
            last_activity_item_id: Some("later_item".to_owned()),
            last_activity_item_type: Some("agent_message".to_owned()),
            last_activity_at_unix: at_unix,
        }
    }

    #[test]
    fn provider_stream_item_defaults_allow_quarter_hour_idle_window() {
        let provider_policy = ProviderTimeoutPolicy::from_secs(5, 180, 180, 120, None);
        let registry = TimeoutPolicyRegistry::with_provider_timeout_policy(provider_policy);

        let reasoning = registry.policy_for(TurnItemType::Reasoning);
        let agent_message = registry.policy_for(TurnItemType::AgentMessage);

        assert_eq!(reasoning, agent_message);
        assert_eq!(reasoning.lease_secs, 16 * 60);
        assert_eq!(reasoning.idle_secs, 15 * 60);
        assert_eq!(reasoning.hard_secs, 30 * 60);
    }

    #[test]
    fn provider_transport_timeout_policy_does_not_change_stream_item_deadlines() {
        let provider_policy = ProviderTimeoutPolicy::from_secs(5, 30, 45, 120, Some(900));
        let registry = TimeoutPolicyRegistry::with_provider_timeout_policy(provider_policy);

        let reasoning = registry.policy_for(TurnItemType::Reasoning);

        assert_eq!(reasoning.lease_secs, 16 * 60);
        assert_eq!(reasoning.idle_secs, 15 * 60);
        assert_eq!(reasoning.hard_secs, 30 * 60);
    }

    #[test]
    fn provider_stream_item_timeout_config_overrides_reasoning_and_agent_message() {
        let registry = TimeoutPolicyRegistry::with_provider_and_command_execution_timeout_policy(
            ProviderTimeoutPolicy::from_secs(5, 30, 45, 120, Some(900)),
            GatewayCommandExecutionTimeoutConfig::default(),
            GatewayProviderStreamItemTimeoutConfig {
                lease_secs: 123,
                idle_secs: 456,
                hard_secs: 789,
            },
        );

        let reasoning = registry.policy_for(TurnItemType::Reasoning);
        let agent_message = registry.policy_for(TurnItemType::AgentMessage);

        assert_eq!(reasoning, agent_message);
        assert_eq!(
            reasoning,
            TimeoutPolicy {
                lease_secs: 123,
                idle_secs: 456,
                hard_secs: 789,
            }
        );
    }

    #[test]
    fn command_execution_defaults_allow_hour_long_shell_commands() {
        let registry = TimeoutPolicyRegistry::default();
        let policy = registry.policy_for(TurnItemType::CommandExecution);
        let (lease_expires_at, idle_deadline_at, hard_deadline_at) = policy.deadlines(1_000);

        assert_eq!(policy.lease_secs, 10 * 60);
        assert_eq!(policy.idle_secs, 30 * 60);
        assert_eq!(policy.hard_secs, 60 * 60);
        assert_eq!(lease_expires_at, 1_600);
        assert_eq!(idle_deadline_at, 2_800);
        assert_eq!(hard_deadline_at, 4_600);
    }

    #[test]
    fn context_compaction_uses_its_configured_finite_policy_and_recovery_grace() {
        let registry = TimeoutPolicyRegistry::with_configured_timeout_policies(
            ProviderTimeoutPolicy::from_secs(5, 30, 45, 120, Some(900)),
            GatewayCommandExecutionTimeoutConfig::default(),
            GatewayProviderStreamItemTimeoutConfig::default(),
            GatewayContextCompactionTimeoutConfig {
                lease_secs: 120,
                idle_secs: 300,
                hard_secs: 1_800,
                recovery_grace_secs: 600,
            },
        );
        let item = TurnItem::SystemEvent {
            id: "context-compaction".to_owned(),
            level: SystemEventLevel::Info,
            message: "Context compaction started".to_owned(),
            code: Some("agent_context_compaction".to_owned()),
            details: Some(serde_json::json!({
                "nativeItemKind": "contextCompaction",
                "status": "started"
            })),
        };

        let policy = registry.policy_for_item(&item);
        let (lease_expires_at, idle_deadline_at, hard_deadline_at) = policy.deadlines(1_000);

        assert_eq!(
            item.execution_class(),
            TurnItemExecutionClass::ContextCompaction
        );
        assert_eq!(lease_expires_at, 1_120);
        assert_eq!(idle_deadline_at, 1_300);
        assert_eq!(hard_deadline_at, 2_800);
        assert_eq!(
            registry.recovery_grace_secs(item.execution_class()),
            Some(600)
        );
    }

    #[test]
    fn ordinary_system_event_keeps_default_timeout() {
        let registry = TimeoutPolicyRegistry::default();
        let item = TurnItem::SystemEvent {
            id: "ordinary-event".to_owned(),
            level: SystemEventLevel::Info,
            message: "Regular event".to_owned(),
            code: Some("agent_runtime_item".to_owned()),
            details: None,
        };

        let policy = registry.policy_for_item(&item);
        let (lease_expires_at, idle_deadline_at, hard_deadline_at) = policy.deadlines(1_000);

        assert_eq!(lease_expires_at, 1_030);
        assert_eq!(idle_deadline_at, 1_030);
        assert_eq!(hard_deadline_at, 1_120);
        assert_eq!(item.execution_class(), TurnItemExecutionClass::Standard);
        assert_eq!(registry.recovery_grace_secs(item.execution_class()), None);
    }

    #[test]
    fn timeout_classifier_recovers_when_no_later_turn_liveness_exists() {
        let candidate = timeout_candidate();

        assert_eq!(
            classify_timeout_candidate_liveness(&candidate, None),
            TimeoutRecoveryClassification::RecoverTurn
        );
        assert_eq!(
            classify_timeout_candidate_liveness(&candidate, Some(&turn_liveness(1_000, 4))),
            TimeoutRecoveryClassification::RecoverTurn
        );
    }

    #[test]
    fn timeout_classifier_suppresses_recovery_after_later_turn_activity() {
        let candidate = timeout_candidate();
        let liveness = turn_liveness(1_010, 5);

        assert!(matches!(
            classify_timeout_candidate_liveness(&candidate, Some(&liveness)),
            TimeoutRecoveryClassification::SuppressRecoveryBecauseTurnProgressed { .. }
        ));
    }

    #[test]
    fn timeout_classifier_uses_sequence_for_same_second_activity_without_later_heartbeat() {
        let candidate = timeout_candidate();
        let liveness = turn_liveness(1_000, 5);

        assert!(matches!(
            classify_timeout_candidate_liveness(&candidate, Some(&liveness)),
            TimeoutRecoveryClassification::SuppressRecoveryBecauseTurnProgressed { .. }
        ));
    }

    #[test]
    fn timeout_classifier_does_not_use_sequence_fallback_after_later_heartbeat() {
        let mut candidate = timeout_candidate();
        candidate.last_heartbeat_at_unix = Some(1_010);
        let liveness = turn_liveness(1_010, 5);

        assert_eq!(
            classify_timeout_candidate_liveness(&candidate, Some(&liveness)),
            TimeoutRecoveryClassification::RecoverTurn
        );
    }
}
