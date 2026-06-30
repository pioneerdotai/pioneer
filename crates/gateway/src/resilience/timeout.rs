use anyhow::Result;
use pioneer_config::{
    GatewayCommandExecutionTimeoutConfig, GatewayProviderStreamItemTimeoutConfig,
};
use pioneer_crud::{CrudStore, TimeoutCandidate, TurnItemAttemptDeadlines};
use pioneer_protocol::{TurnItem, TurnItemType};
use pioneer_provider::ProviderTimeoutPolicy;
use std::collections::HashMap;
use std::sync::Arc;

const CLI_CONTEXT_COMPACTION_TIMEOUT_SECS: u64 = 5 * 60;
const CLI_CONTEXT_COMPACTION_HARD_TIMEOUT_SECS: u64 = 10 * 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutPolicy {
    pub lease_secs: u64,
    pub idle_secs: u64,
    pub hard_secs: u64,
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
        Self { by_item_type }
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

    pub fn with_provider_and_command_execution_timeout_policy(
        _provider_policy: ProviderTimeoutPolicy,
        command_execution_config: GatewayCommandExecutionTimeoutConfig,
        provider_stream_item_config: GatewayProviderStreamItemTimeoutConfig,
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
        if is_cli_native_context_compaction_item(item) {
            return TimeoutPolicy {
                lease_secs: CLI_CONTEXT_COMPACTION_TIMEOUT_SECS,
                idle_secs: CLI_CONTEXT_COMPACTION_TIMEOUT_SECS,
                hard_secs: CLI_CONTEXT_COMPACTION_HARD_TIMEOUT_SECS,
            };
        }

        self.policy_for(item.item_type())
    }
}

#[derive(Clone)]
pub struct TimeoutSupervisor {
    crud_store: Arc<CrudStore>,
    policy_registry: TimeoutPolicyRegistry,
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
        let deadlines = self
            .deadlines_for_stored_item(turn_id, item_id, item_type, now_unix)
            .await?;
        let _ = self
            .crud_store
            .heartbeat_turn_item_attempt(
                turn_id,
                item_id,
                now_unix,
                deadlines.lease_expires_at_unix,
                deadlines.idle_deadline_at_unix,
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

    pub async fn renew_running_attempt_deadlines_for_turn(
        &self,
        turn_id: &str,
        now_unix: i64,
    ) -> Result<usize> {
        let attempts = self
            .crud_store
            .list_running_turn_item_attempts_for_turn(turn_id)
            .await?;
        let mut renewed = 0usize;
        for attempt in attempts {
            let deadlines = self
                .deadlines_for_stored_item(
                    attempt.turn_id.as_str(),
                    attempt.item_id.as_str(),
                    attempt.item_type,
                    now_unix,
                )
                .await?;
            if self
                .crud_store
                .configure_turn_item_attempt_deadlines(
                    attempt.turn_id.as_str(),
                    attempt.item_id.as_str(),
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
        let policy = match self.crud_store.get_turn_item(turn_id, item_id).await? {
            Some(item) => self.policy_registry.policy_for_item(&item),
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
}

fn is_cli_native_context_compaction_item(item: &TurnItem) -> bool {
    let TurnItem::SystemEvent { code, details, .. } = item else {
        return false;
    };

    if code.as_deref() != Some("agent_context_compaction") {
        return false;
    }

    details
        .as_ref()
        .and_then(|details| details.get("nativeItemKind"))
        .and_then(|value| value.as_str())
        == Some("contextCompaction")
}

fn saturating_add_secs(base: i64, seconds: u64) -> i64 {
    base.saturating_add(i64::try_from(seconds).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{SystemEventLevel, TurnItem, TurnItemType};

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
    fn cli_native_context_compaction_uses_extended_timeout() {
        let registry = TimeoutPolicyRegistry::default();
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

        assert_eq!(lease_expires_at, 1_300);
        assert_eq!(idle_deadline_at, 1_300);
        assert_eq!(hard_deadline_at, 1_600);
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
    }
}
