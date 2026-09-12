//! Durable, deterministic state transitions for one compaction operation.
//! The integration layer persists each transition with CAS before performing
//! its action. Source payloads and summary text are never copied into this state.
use crate::{ATTEMPT_MILLIS, ModelBudget};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceCursor {
    pub unit: u64,
    pub source: u32,
    pub character: u64,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum AttemptPurpose {
    Portion,
    Correction,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum FailureKind {
    Transient,
    Permanent,
    InvalidCompletion,
    InsufficientEffect,
    Deadline,
    Cancelled,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum RunnerPhase {
    Ready {
        purpose: AttemptPurpose,
    },
    Attempt {
        number: u64,
        purpose: AttemptPurpose,
        deadline_ms: u64,
    },
    Backoff {
        purpose: AttemptPurpose,
        not_before_ms: u64,
    },
    Candidate {
        checkpoint: String,
        final_portion: bool,
    },
    Commit {
        checkpoint: String,
    },
    Applied {
        checkpoint: String,
    },
    Failed {
        kind: FailureKind,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttemptObservation {
    pub number: u64,
    pub started_ms: u64,
    pub finished_ms: Option<u64>,
    pub estimated_input_tokens: u64,
    pub output_cap: u64,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub completion: Option<crate::summary::CompletionKind>,
    pub failure: Option<FailureKind>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunnerState {
    pub generation: u64,
    pub deadline_ms: u64,
    pub attempts: u64,
    pub retries: u8,
    pub corrections: u8,
    pub target_tokens: u64,
    pub cursor: SourceCursor,
    pub previous_checkpoint: Option<String>,
    pub phase: RunnerPhase,
    #[serde(default)]
    pub observation: Option<AttemptObservation>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunnerAction {
    Prepare(AttemptPurpose),
    WaitUntil(u64),
    RecoverInterruptedAttempt,
    ValidateCandidate {
        checkpoint: String,
        final_portion: bool,
    },
    Commit(String),
    Terminal,
    Expired,
}

impl RunnerState {
    pub fn new(
        deadline_ms: u64,
        model: &ModelBudget,
        goal: u64,
        previous_checkpoint: Option<String>,
    ) -> anyhow::Result<Self> {
        let target_tokens = model.summarizer_cap(goal)?;
        anyhow::ensure!(target_tokens > 0, "summarizer has no output capacity");
        Ok(Self {
            observation: None,
            generation: 0,
            deadline_ms,
            attempts: 0,
            retries: 0,
            corrections: 0,
            target_tokens,
            cursor: SourceCursor::default(),
            previous_checkpoint,
            phase: RunnerPhase::Ready {
                purpose: AttemptPurpose::Portion,
            },
        })
    }
    pub fn action(&self, now_ms: u64) -> RunnerAction {
        if matches!(
            self.phase,
            RunnerPhase::Applied { .. } | RunnerPhase::Failed { .. }
        ) {
            return RunnerAction::Terminal;
        }
        if now_ms >= self.deadline_ms {
            return RunnerAction::Expired;
        }
        match &self.phase {
            RunnerPhase::Ready { purpose } => RunnerAction::Prepare(*purpose),
            RunnerPhase::Backoff {
                purpose,
                not_before_ms,
            } if now_ms >= *not_before_ms => RunnerAction::Prepare(*purpose),
            RunnerPhase::Backoff { not_before_ms, .. } => RunnerAction::WaitUntil(*not_before_ms),
            RunnerPhase::Attempt { .. } => RunnerAction::RecoverInterruptedAttempt,
            RunnerPhase::Candidate {
                checkpoint,
                final_portion,
            } => RunnerAction::ValidateCandidate {
                checkpoint: checkpoint.clone(),
                final_portion: *final_portion,
            },
            RunnerPhase::Commit { checkpoint } => RunnerAction::Commit(checkpoint.clone()),
            RunnerPhase::Applied { .. } | RunnerPhase::Failed { .. } => RunnerAction::Terminal,
        }
    }
    /// Persist the returned state before making any transport call. In-flight
    /// attempts are recovered as interrupted attempts, never silently reset.
    pub fn claim(&self, now_ms: u64) -> anyhow::Result<Self> {
        let RunnerAction::Prepare(purpose) = self.action(now_ms) else {
            anyhow::bail!("attempt is not admitted")
        };
        let mut next = self.next()?;
        next.attempts = next
            .attempts
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("attempt counter overflow"))?;
        next.observation = None;
        next.phase = RunnerPhase::Attempt {
            number: next.attempts,
            purpose,
            deadline_ms: self.deadline_ms.min(now_ms.saturating_add(ATTEMPT_MILLIS)),
        };
        Ok(next)
    }
    pub fn attempt_failed(
        &self,
        kind: FailureKind,
        now_ms: u64,
        retry_after_ms: Option<u64>,
    ) -> anyhow::Result<Self> {
        let RunnerPhase::Attempt { purpose, .. } = self.phase else {
            anyhow::bail!("no current attempt")
        };
        let mut next = self.next()?;
        if let Some(observation) = &mut next.observation {
            observation.finished_ms = Some(now_ms);
            observation.failure = Some(kind.clone());
        }
        if now_ms >= self.deadline_ms {
            next.phase = RunnerPhase::Failed {
                kind: FailureKind::Deadline,
            };
        } else if kind == FailureKind::Transient && self.retries < 2 {
            let delay = [2_000_u64, 8_000][self.retries as usize].max(retry_after_ms.unwrap_or(0));
            let not_before_ms = now_ms.saturating_add(delay);
            if not_before_ms >= self.deadline_ms {
                next.phase = RunnerPhase::Failed {
                    kind: FailureKind::Deadline,
                };
            } else {
                next.retries += 1;
                next.phase = RunnerPhase::Backoff {
                    purpose,
                    not_before_ms,
                };
            }
        } else {
            next.phase = RunnerPhase::Failed { kind };
        }
        Ok(next)
    }
    /// Candidate text and this state must be committed in the same transaction.
    /// Coverage is supplied separately by the source materializer, only for
    /// fully consumed units; an intermediate fragment is not coverage.
    pub fn candidate(
        &self,
        attempt: u64,
        checkpoint: String,
        cursor: SourceCursor,
        final_portion: bool,
        now_ms: u64,
    ) -> anyhow::Result<Self> {
        let RunnerPhase::Attempt {
            number,
            purpose,
            deadline_ms,
        } = self.phase
        else {
            anyhow::bail!("no current attempt")
        };
        anyhow::ensure!(
            attempt == number && now_ms < deadline_ms && now_ms < self.deadline_ms,
            "late or superseded completion"
        );
        anyhow::ensure!(
            purpose == AttemptPurpose::Correction || cursor > self.cursor,
            "portion made no source progress"
        );
        anyhow::ensure!(
            purpose != AttemptPurpose::Correction || (cursor == self.cursor && final_portion),
            "correction changed source coverage"
        );
        let mut next = self.next()?;
        next.cursor = cursor;
        next.previous_checkpoint = Some(checkpoint.clone());
        next.phase = RunnerPhase::Candidate {
            checkpoint,
            final_portion,
        };
        Ok(next)
    }
    pub fn candidate_checked(&self, target_fits: bool) -> anyhow::Result<Self> {
        let RunnerPhase::Candidate {
            checkpoint,
            final_portion,
        } = &self.phase
        else {
            anyhow::bail!("no candidate to validate")
        };
        let mut next = self.next()?;
        next.phase = if !final_portion {
            RunnerPhase::Ready {
                purpose: AttemptPurpose::Portion,
            }
        } else if target_fits {
            RunnerPhase::Commit {
                checkpoint: checkpoint.clone(),
            }
        } else if self.corrections == 0 && self.target_tokens > 1 {
            next.corrections = 1;
            next.target_tokens = (self.target_tokens / 2).max(1);
            RunnerPhase::Ready {
                purpose: AttemptPurpose::Correction,
            }
        } else {
            RunnerPhase::Failed {
                kind: FailureKind::InsufficientEffect,
            }
        };
        Ok(next)
    }
    /// The store calls this only inside the transaction which applies the head CAS.
    pub fn applied(&self, checkpoint: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(
            matches!(&self.phase, RunnerPhase::Commit { checkpoint: expected } if expected == checkpoint),
            "checkpoint is not admitted for commit"
        );
        let mut next = self.next()?;
        next.phase = RunnerPhase::Applied {
            checkpoint: checkpoint.into(),
        };
        Ok(next)
    }
    pub fn terminate(&self, kind: FailureKind) -> anyhow::Result<Self> {
        if matches!(
            self.phase,
            RunnerPhase::Applied { .. } | RunnerPhase::Failed { .. }
        ) {
            return Ok(self.clone());
        }
        let mut next = self.next()?;
        next.phase = RunnerPhase::Failed { kind };
        Ok(next)
    }
    fn next(&self) -> anyhow::Result<Self> {
        let mut next = self.clone();
        next.generation = self
            .generation
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("state generation overflow"))?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn state() -> RunnerState {
        RunnerState::new(
            900_000,
            &ModelBudget::new(Some(128_000), None, None),
            10_000,
            None,
        )
        .unwrap()
    }
    fn restart(s: &RunnerState) -> RunnerState {
        serde_json::from_str(&serde_json::to_string(s).unwrap()).unwrap()
    }
    #[test]
    fn retries_and_retry_after_survive_restart_and_do_not_reset_deadline() {
        let first = state().claim(0).unwrap();
        assert_eq!(first.action(1), RunnerAction::RecoverInterruptedAttempt);
        let waiting = restart(&first)
            .attempt_failed(FailureKind::Transient, 10, Some(4_000))
            .unwrap();
        assert_eq!(waiting.action(20), RunnerAction::WaitUntil(4_010));
        assert!(waiting.claim(4_009).is_err());
        let second = restart(&waiting).claim(4_010).unwrap();
        let waiting = second
            .attempt_failed(FailureKind::Transient, 4_020, None)
            .unwrap();
        assert_eq!(waiting.action(4_021), RunnerAction::WaitUntil(12_020));
        let third = restart(&waiting).claim(12_020).unwrap();
        assert_eq!(third.attempts, 3);
        assert_eq!(third.retries, 2);
        assert_eq!(third.deadline_ms, 900_000);
        assert!(matches!(
            third
                .attempt_failed(FailureKind::Transient, 12_030, None)
                .unwrap()
                .phase,
            RunnerPhase::Failed { .. }
        ));
        assert_eq!(third.action(900_000), RunnerAction::Expired);
    }
    #[test]
    fn persisted_candidate_reuses_summary_and_one_correction_keeps_coverage() {
        let attempt = state().claim(0).unwrap();
        let cursor = SourceCursor {
            unit: 0,
            source: 0,
            character: 100,
        };
        let intermediate = attempt
            .candidate(1, "part-1".into(), cursor, false, 100)
            .unwrap();
        assert!(matches!(
            restart(&intermediate).action(101),
            RunnerAction::ValidateCandidate { .. }
        ));
        let next = intermediate
            .candidate_checked(false)
            .unwrap()
            .claim(102)
            .unwrap();
        assert_eq!(next.previous_checkpoint.as_deref(), Some("part-1"));
        let cursor = SourceCursor {
            unit: 1,
            source: 0,
            character: 0,
        };
        let last = next
            .candidate(2, "part-2".into(), cursor, true, 200)
            .unwrap();
        let correction = last.candidate_checked(false).unwrap();
        assert_eq!(correction.target_tokens, 5_000);
        let attempt = restart(&correction).claim(201).unwrap();
        let corrected = attempt
            .candidate(3, "corrected".into(), cursor, true, 300)
            .unwrap();
        assert!(matches!(
            corrected.candidate_checked(false).unwrap().phase,
            RunnerPhase::Failed {
                kind: FailureKind::InsufficientEffect
            }
        ));
        let ready = corrected.candidate_checked(true).unwrap();
        assert_eq!(
            restart(&ready).action(301),
            RunnerAction::Commit("corrected".into())
        );
        assert_eq!(
            ready.applied("corrected").unwrap().action(302),
            RunnerAction::Terminal
        );
    }
    #[test]
    fn stop_late_results_zero_progress_and_permanent_failure_never_publish() {
        let attempt = state().claim(0).unwrap();
        assert!(
            attempt
                .candidate(
                    1,
                    "late".into(),
                    SourceCursor {
                        unit: 1,
                        ..Default::default()
                    },
                    true,
                    300_000
                )
                .is_err()
        );
        assert!(
            attempt
                .candidate(1, "stuck".into(), SourceCursor::default(), true, 1)
                .is_err()
        );
        let stopped = attempt.terminate(FailureKind::Cancelled).unwrap();
        assert!(
            stopped
                .candidate(
                    1,
                    "late".into(),
                    SourceCursor {
                        unit: 1,
                        ..Default::default()
                    },
                    true,
                    1
                )
                .is_err()
        );
        assert_eq!(stopped.action(2), RunnerAction::Terminal);
        assert!(matches!(
            attempt
                .attempt_failed(FailureKind::Permanent, 1, None)
                .unwrap()
                .phase,
            RunnerPhase::Failed {
                kind: FailureKind::Permanent
            }
        ));
        let late = state().claim(899_000).unwrap();
        assert!(matches!(
            late.phase,
            RunnerPhase::Attempt {
                deadline_ms: 900_000,
                ..
            }
        ));
        assert!(matches!(
            late.attempt_failed(FailureKind::Transient, 899_001, Some(5000))
                .unwrap()
                .phase,
            RunnerPhase::Failed {
                kind: FailureKind::Deadline
            }
        ));
    }
}
