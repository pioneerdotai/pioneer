use anyhow::Result;
use pioneer_crud::{CrudStore, TimeoutCandidate};
use pioneer_protocol::TurnItemType;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
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
            TimeoutPolicy {
                lease_secs: 180,
                idle_secs: 120,
                hard_secs: 10 * 60,
            },
        );
        by_item_type.insert(
            Reasoning,
            TimeoutPolicy {
                lease_secs: 180,
                idle_secs: 120,
                hard_secs: 10 * 60,
            },
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
            TimeoutPolicy {
                lease_secs: 120,
                idle_secs: 90,
                hard_secs: 10 * 60,
            },
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

    pub async fn register_item_attempt(
        &self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        now_unix: i64,
    ) -> Result<()> {
        let policy = self.policy_registry.policy_for(item_type);
        let (lease_expires_at, idle_deadline_at, hard_deadline_at) = policy.deadlines(now_unix);
        let _ = self
            .crud_store
            .configure_turn_item_attempt_deadlines(
                turn_id,
                item_id,
                now_unix,
                Some(lease_expires_at),
                Some(idle_deadline_at),
                Some(hard_deadline_at),
            )
            .await?;
        Ok(())
    }

    pub async fn heartbeat_item_attempt(
        &self,
        turn_id: &str,
        item_id: &str,
        item_type: TurnItemType,
        now_unix: i64,
    ) -> Result<()> {
        let policy = self.policy_registry.policy_for(item_type);
        let (lease_expires_at, idle_deadline_at, _) = policy.deadlines(now_unix);
        let _ = self
            .crud_store
            .heartbeat_turn_item_attempt(
                turn_id,
                item_id,
                now_unix,
                Some(lease_expires_at),
                Some(idle_deadline_at),
            )
            .await?;
        Ok(())
    }

    pub async fn poll_timeouts(&self, now_unix: i64, limit: u64) -> Result<Vec<TimeoutCandidate>> {
        let candidates = self
            .crud_store
            .list_timeout_candidates(now_unix, limit)
            .await?;
        let mut timed_out = Vec::new();
        for candidate in candidates {
            let transitioned = self
                .crud_store
                .transition_timeout_candidate(&candidate, now_unix)
                .await?;
            if transitioned {
                timed_out.push(candidate);
            }
        }
        Ok(timed_out)
    }
}

fn saturating_add_secs(base: i64, seconds: u64) -> i64 {
    base.saturating_add(i64::try_from(seconds).unwrap_or(i64::MAX))
}
