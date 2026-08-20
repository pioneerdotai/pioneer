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
    #[cfg(test)]
    ObservationUnavailable,
    #[cfg(test)]
    IdleDeadlineExceeded,
    #[cfg(test)]
    HardDeadlineExceeded,
    #[cfg(test)]
    Active,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutionObservation {
    #[cfg(test)]
    Progress,
    Heartbeat,
    #[cfg(test)]
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionLivenessAdapter {
    state: ExecutionAttemptState,
    idle_extension: Duration,
    #[cfg(test)]
    observation_available: bool,
    #[cfg(test)]
    provider_owned_operation: bool,
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
            #[cfg(test)]
            observation_available: true,
            #[cfg(test)]
            provider_owned_operation: false,
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
        #[cfg(test)]
        if matches!(observation, ExecutionObservation::Unavailable) {
            self.observation_available = false;
            return ExecutionLivenessDecision::ObservationUnavailable;
        }
        #[cfg(test)]
        {
            self.observation_available = true;
        }
        let result = match observation {
            #[cfg(test)]
            ExecutionObservation::Progress => self.state.record_progress(now),
            ExecutionObservation::Heartbeat => self.state.record_heartbeat(now),
            #[cfg(test)]
            ExecutionObservation::Unavailable => unreachable!(),
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

    fn set_provider_owned_operation(&mut self, active: bool) {
        self.provider_owned_operation = active;
    }

    fn generic_item_timeout_applies(&self) -> bool {
        !self.provider_owned_operation
    }

    fn timeout_decision(
        &self,
        execution_id: &AgentExecutionId,
        attempt_generation: u64,
        now: DateTime<FixedOffset>,
    ) -> ExecutionLivenessDecision {
        if execution_id != &self.state.execution_id
            || attempt_generation != self.state.attempt_generation
            || self.state.fenced
        {
            return ExecutionLivenessDecision::StaleAttempt;
        }
        if self
            .state
            .hard_deadline
            .is_some_and(|deadline| now >= deadline)
        {
            return ExecutionLivenessDecision::HardDeadlineExceeded;
        }
        if !self.observation_available {
            return ExecutionLivenessDecision::ObservationUnavailable;
        }
        if self
            .state
            .idle_deadline
            .is_some_and(|deadline| now >= deadline)
        {
            return ExecutionLivenessDecision::IdleDeadlineExceeded;
        }
        ExecutionLivenessDecision::Active
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

    #[test]
    fn missing_observation_defers_timeout_and_hard_deadline_is_not_extended() {
        let (execution, _) = ids();
        let hard = at(100);
        let mut adapter =
            ExecutionLivenessAdapter::new(execution.clone(), 1, Some(at(10)), Some(hard), 20)
                .unwrap();
        assert_eq!(
            adapter.observe(&execution, 1, ExecutionObservation::Unavailable, at(50)),
            ExecutionLivenessDecision::ObservationUnavailable
        );
        assert_eq!(
            adapter.timeout_decision(&execution, 1, at(60)),
            ExecutionLivenessDecision::ObservationUnavailable
        );
        assert_eq!(
            adapter.timeout_decision(&execution, 1, at(100)),
            ExecutionLivenessDecision::HardDeadlineExceeded
        );
        assert_eq!(
            adapter.observe(&execution, 1, ExecutionObservation::Progress, at(70)),
            ExecutionLivenessDecision::Renewed
        );
        assert_eq!(adapter.state().hard_deadline, Some(hard));
        assert_eq!(
            adapter.timeout_decision(&execution, 1, at(100)),
            ExecutionLivenessDecision::HardDeadlineExceeded
        );
    }

    #[test]
    fn provider_owned_long_operation_is_not_killed_by_generic_item_timer() {
        let (execution, _) = ids();
        let mut adapter = ExecutionLivenessAdapter::new(execution, 1, None, None, 20).unwrap();
        assert!(adapter.generic_item_timeout_applies());
        adapter.set_provider_owned_operation(true);
        assert!(!adapter.generic_item_timeout_applies());
    }
}
