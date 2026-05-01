use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct McpRetryPolicy {
    pub initial_delay_secs: u64,
    pub max_delay_secs: u64,
    pub multiplier: u64,
}

impl Default for McpRetryPolicy {
    fn default() -> Self {
        Self {
            initial_delay_secs: 1,
            max_delay_secs: 60,
            multiplier: 2,
        }
    }
}

impl McpRetryPolicy {
    pub fn delay_secs(self, retry_attempt: u32) -> u64 {
        if retry_attempt == 0 {
            return self.initial_delay_secs;
        }

        let mut delay = self.initial_delay_secs.max(1);
        for _ in 0..retry_attempt {
            delay = delay.saturating_mul(self.multiplier.max(1));
            if delay >= self.max_delay_secs {
                return self.max_delay_secs;
            }
        }
        delay.min(self.max_delay_secs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_caps() {
        let policy = McpRetryPolicy {
            initial_delay_secs: 1,
            max_delay_secs: 8,
            multiplier: 2,
        };

        assert_eq!(policy.delay_secs(0), 1);
        assert_eq!(policy.delay_secs(1), 2);
        assert_eq!(policy.delay_secs(2), 4);
        assert_eq!(policy.delay_secs(3), 8);
        assert_eq!(policy.delay_secs(4), 8);
    }
}
