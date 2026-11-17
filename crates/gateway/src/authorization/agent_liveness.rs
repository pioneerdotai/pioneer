//! Execution-local liveness for agent domain attempts.
//!
//! A root resource scope is shared for accounting, but this module never uses
//! activity from a parent, sibling or root as evidence for another execution.

use super::ExecutionAttemptState;
use chrono::{DateTime, Duration, FixedOffset};
use pioneer_protocol::AgentExecutionId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionLivenessDecision {
    Renewed,
    StaleAttempt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionObservation {
    #[cfg(test)]
    Progress,
    Heartbeat,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionLivenessAdapter {
    state: ExecutionAttemptState,
    idle_extension: Duration,
}

impl ExecutionLivenessAdapter {
    pub(crate) fn new(
        execution_id: AgentExecutionId,
        attempt_generation: u64,
        idle_deadline: Option<DateTime<FixedOffset>>,
        hard_deadline: Option<DateTime<FixedOffset>>,
        idle_extension_secs: u64,
    ) -> Result<Self, super::ExecutionMaterializationError> {
        Ok(Self {
            state: ExecutionAttemptState::new(
                execution_id,
                attempt_generation,
                idle_deadline,
                hard_deadline,
            )?,
            idle_extension: Duration::seconds(idle_extension_secs.min(i64::MAX as u64) as i64),
        })
    }

    pub(crate) fn observe(
        &mut self,
        execution_id: &AgentExecutionId,
        attempt_generation: u64,
        observation: ExecutionObservation,
        now: DateTime<FixedOffset>,
    ) -> ExecutionLivenessDecision {
        if execution_id != &self.state.execution_id
            || attempt_generation != self.state.attempt_generation
            || self.state.fenced
        {
            return ExecutionLivenessDecision::StaleAttempt;
        }
        let result = match observation {
            #[cfg(test)]
            ExecutionObservation::Progress => self.state.record_progress(now),
            ExecutionObservation::Heartbeat => self.state.record_heartbeat(now),
        };
        if result.is_err() {
            return ExecutionLivenessDecision::StaleAttempt;
        }
        // Progress and heartbeat renew only this execution's idle frontier.
        // The hard deadline is intentionally never recomputed here.
        let renewed_idle = now + self.idle_extension;
        let renewed_idle = self
            .state
            .hard_deadline
            .map_or(renewed_idle, |hard| renewed_idle.min(hard));
        self.state.idle_deadline = Some(
            self.state
                .idle_deadline
                .map_or(renewed_idle, |current| current.max(renewed_idle)),
        );
        ExecutionLivenessDecision::Renewed
    }
}

#[cfg(test)]
impl ExecutionLivenessAdapter {
    fn state(&self) -> &ExecutionAttemptState {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ids() -> (AgentExecutionId, AgentExecutionId) {
        (
            AgentExecutionId::new("E12345678901234567890").unwrap(),
            AgentExecutionId::new("E12345678901234567891").unwrap(),
        )
    }

    fn at(seconds: i64) -> DateTime<FixedOffset> {
        chrono::Utc
            .timestamp_opt(seconds, 0)
            .single()
            .unwrap()
            .fixed_offset()
    }

    #[test]
    fn progress_and_heartbeat_renew_only_exact_execution_idle_frontier() {
        let (execution, sibling) = ids();
        let hard = at(100);
        let mut first =
            ExecutionLivenessAdapter::new(execution.clone(), 1, Some(at(10)), Some(hard), 20)
                .unwrap();
        assert_eq!(
            first.observe(&sibling, 1, ExecutionObservation::Progress, at(20)),
            ExecutionLivenessDecision::StaleAttempt
        );
        assert_eq!(first.state().progress_sequence, 0);
        assert_eq!(
            first.observe(&execution, 1, ExecutionObservation::Progress, at(20)),
            ExecutionLivenessDecision::Renewed
        );
        assert_eq!(first.state().progress_sequence, 1);
        assert_eq!(first.state().hard_deadline, Some(hard));
    }
}
