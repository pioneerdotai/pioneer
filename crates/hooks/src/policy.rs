use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookAwaitPolicy {
    Blocking,
    Deadline,
    Background,
    FireAndRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookFailurePolicy {
    Required,
    Fallback,
    BestEffort,
    Skip,
    FailClosed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HookRetryBackoff {
    None,
    Fixed,
    Exponential,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookExecutionPolicy {
    pub await_policy: HookAwaitPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallelism: Option<u16>,
}

impl Default for HookExecutionPolicy {
    fn default() -> Self {
        Self {
            await_policy: HookAwaitPolicy::Deadline,
            timeout_ms: Some(1_000),
            max_parallelism: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookRetryPolicy {
    pub max_attempts: u16,
    pub backoff: HookRetryBackoff,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_delay_ms: Option<u64>,
    pub idempotency_required: bool,
}

impl Default for HookRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            backoff: HookRetryBackoff::None,
            initial_delay_ms: None,
            idempotency_required: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_policy_default_is_bounded() {
        let policy = HookExecutionPolicy::default();
        assert_eq!(policy.await_policy, HookAwaitPolicy::Deadline);
        assert_eq!(policy.timeout_ms, Some(1_000));
        assert_eq!(policy.max_parallelism, None);
    }

    #[test]
    fn retry_policy_default_is_no_retry() {
        let policy = HookRetryPolicy::default();
        assert_eq!(policy.max_attempts, 1);
        assert_eq!(policy.backoff, HookRetryBackoff::None);
        assert!(policy.idempotency_required);
    }

    #[test]
    fn await_policy_serializes_stably() {
        assert_eq!(
            serde_json::to_value(HookAwaitPolicy::FireAndRecord).expect("policy serializes"),
            serde_json::json!("fire_and_record")
        );
    }

    #[test]
    fn failure_policy_serializes_stably() {
        assert_eq!(
            serde_json::to_value(HookFailurePolicy::BestEffort).expect("policy serializes"),
            serde_json::json!("best_effort")
        );
    }
}
