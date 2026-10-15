//! Durable, deterministic state transitions for one compaction operation.
//! The integration layer persists each transition with CAS before performing
//! its action. Source payloads and summary text are never copied into this state.
use crate::{ATTEMPT_MILLIS, ModelBudget};
use serde::{Deserialize, Serialize};

/// The next source-text representation after main's historical-command v1.
pub const PORTABLE_CONTEXT_SOURCE_TEXT_PROJECTION_VERSION: u32 = 2;
/// Adds typed legacy Task-basis projection without changing v0/v1/v2 cursor offsets.
pub const PORTABLE_TASK_BASIS_SOURCE_TEXT_PROJECTION_VERSION: u32 = 3;
/// Adds typed commentary projection while preserving every earlier cursor representation.
pub const COMMENTARY_SOURCE_TEXT_PROJECTION_VERSION: u32 = 4;

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
/// Durable operational diagnostics contain only classified metadata, never raw
/// provider/transport errors, request bodies, paths or credentials.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailureDiagnostic {
    pub stage: String,
    pub code: String,
    pub explanation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_code: Option<i64>,
}
impl FailureDiagnostic {
    pub fn new(stage: &str, code: &str, explanation: &str) -> Self {
        Self {
            stage: stage.into(),
            code: code.into(),
            explanation: explanation.into(),
            rpc_code: None,
        }
    }
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
    #[serde(default)]
    pub diagnostic: Option<FailureDiagnostic>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunnerState {
    pub generation: u64,
    pub deadline_ms: u64,
    pub attempts: u64,
    pub retries: u8,
    /// Claims for the current portion (or correction), persisted before I/O.
    /// Accepted candidates and an admitted Deadline-resume reset this
    /// independently of operation-wide retries.
    #[serde(default)]
    pub portion_attempts: u8,
    pub corrections: u8,
    pub target_tokens: u64,
    /// Text representation used by persisted character cursors. Version zero
    /// is the legacy canonical payload and is retained for pre-upgrade runs.
    #[serde(default)]
    pub source_text_projection_version: u32,
    pub cursor: SourceCursor,
    pub previous_checkpoint: Option<String>,
    pub phase: RunnerPhase,
    /// Phase before a deadline fence; older records recover from their saved cursor.
    #[serde(default)]
    pub resume_phase: Option<RunnerPhase>,
    #[serde(default)]
    pub observation: Option<AttemptObservation>,
    #[serde(default)]
    pub diagnostic: Option<FailureDiagnostic>,
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
            resume_phase: None,
            observation: None,
            diagnostic: None,
            generation: 0,
            deadline_ms,
            attempts: 0,
            retries: 0,
            portion_attempts: 0,
            corrections: 0,
            target_tokens,
            source_text_projection_version: COMMENTARY_SOURCE_TEXT_PROJECTION_VERSION,
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
        // Legacy Backoff records lack the new counter. Their scheduled
        // transient retries conservatively bound calls already consumed.
        let consumed =
            if self.portion_attempts == 0 && matches!(self.phase, RunnerPhase::Backoff { .. }) {
                self.retries
            } else {
                self.portion_attempts
            };
        anyhow::ensure!(consumed < 3, "portion attempt limit exhausted");
        let mut next = self.next()?;
        next.portion_attempts = consumed + 1;
        next.attempts = next
            .attempts
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("attempt counter overflow"))?;
        next.observation = None;
        next.diagnostic = None;
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
        // Legacy Attempt records include their current call plus the already
        // scheduled transient retries. Never grant extra calls on upgrade.
        next.portion_attempts = if self.portion_attempts == 0 {
            self.retries.saturating_add(1)
        } else {
            self.portion_attempts
        };
        if now_ms >= self.deadline_ms {
            next.phase = RunnerPhase::Failed {
                kind: FailureKind::Deadline,
            };
        } else if next.portion_attempts < 3
            && (kind == FailureKind::InvalidCompletion
                || (kind == FailureKind::Transient && self.retries < 2))
        {
            let retry = if kind == FailureKind::Transient {
                self.retries
            } else {
                next.portion_attempts - 1
            };
            let delay = [2_000_u64, 8_000][retry as usize].max(retry_after_ms.unwrap_or(0));
            let not_before_ms = now_ms.saturating_add(delay);
            if not_before_ms >= self.deadline_ms {
                next.phase = RunnerPhase::Failed {
                    kind: FailureKind::Deadline,
                };
            } else {
                if kind == FailureKind::Transient {
                    next.retries += 1;
                }
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
        next.portion_attempts = 0;
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
        if kind == FailureKind::Deadline {
            next.resume_phase = Some(match &self.phase {
                RunnerPhase::Attempt { purpose, .. } => RunnerPhase::Ready { purpose: *purpose },
                phase => phase.clone(),
            });
        }
        next.phase = RunnerPhase::Failed { kind };
        Ok(next)
    }
    pub fn can_resume_deadline(&self) -> bool {
        matches!(
            self.phase,
            RunnerPhase::Failed {
                kind: FailureKind::Deadline
            }
        ) && self.cursor > SourceCursor::default()
            && self.previous_checkpoint.is_some()
            && (self.resume_phase.is_some() || self.corrections == 0)
            && self
                .observation
                .as_ref()
                .is_none_or(|o| o.failure.is_none())
    }
    /// A later admission supplies its existing request budget. Never extend the
    /// interrupted request or reset provider retry/correction budgets. Only
    /// this admitted transition grants a fresh portion budget for a new execution.
    pub fn resume_deadline(&self, deadline_ms: u64, legacy_final: bool) -> anyhow::Result<Self> {
        anyhow::ensure!(
            self.can_resume_deadline() && deadline_ms > self.deadline_ms,
            "no resumable deadline progress"
        );
        let mut next = self.next()?;
        next.deadline_ms = deadline_ms;
        next.portion_attempts = 0;
        next.phase = next.resume_phase.take().unwrap_or_else(|| {
            if legacy_final {
                RunnerPhase::Candidate {
                    checkpoint: self.previous_checkpoint.clone().unwrap(),
                    final_portion: true,
                }
            } else {
                RunnerPhase::Ready {
                    purpose: AttemptPurpose::Portion,
                }
            }
        });
        next.observation = None;
        next.diagnostic = None;
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
    #[test]
    fn deadline_resume_keeps_final_candidate_and_rejects_nonrecoverable_failures() {
        let candidate = state()
            .claim(1)
            .unwrap()
            .candidate(
                1,
                "saved".into(),
                SourceCursor {
                    unit: 1,
                    ..Default::default()
                },
                true,
                2,
            )
            .unwrap();
        let failed = candidate.terminate(FailureKind::Deadline).unwrap();
        let resumed = restart(&failed).resume_deadline(1_800_000, false).unwrap();
        assert_eq!(resumed.phase, candidate.phase);
        assert_eq!(resumed.cursor, candidate.cursor);
        assert_eq!(resumed.attempts, candidate.attempts);
        assert!(failed.resume_deadline(failed.deadline_ms, false).is_err());
        assert!(
            !state()
                .terminate(FailureKind::Deadline)
                .unwrap()
                .can_resume_deadline()
        );
        for kind in [
            FailureKind::Cancelled,
            FailureKind::Permanent,
            FailureKind::Transient,
        ] {
            assert!(!candidate.terminate(kind).unwrap().can_resume_deadline());
        }
        let mut legacy = serde_json::to_value(&failed).unwrap();
        legacy.as_object_mut().unwrap().remove("resume_phase");
        let legacy: RunnerState = serde_json::from_value(legacy).unwrap();
        assert_eq!(
            legacy.resume_deadline(1_800_000, true).unwrap().phase,
            candidate.phase
        );
    }

    #[test]
    fn legacy_runner_json_defaults_missing_diagnostics() {
        let state = state();
        let mut json = serde_json::to_value(&state).unwrap();
        json.as_object_mut().unwrap().remove("diagnostic");
        let restored: RunnerState = serde_json::from_value(json).unwrap();
        assert_eq!(restored, state);
    }

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
    fn fail_and_retry(state: RunnerState, kind: FailureKind, now: u64) -> RunnerState {
        let failed = restart(&state).attempt_failed(kind, now, None).unwrap();
        let RunnerPhase::Backoff { not_before_ms, .. } = failed.phase else {
            panic!("expected retry, got {:?}", failed.phase);
        };
        restart(&failed).claim(not_before_ms).unwrap()
    }

    #[test]
    fn invalid_completions_preserve_progress_and_third_can_succeed() {
        let cursor = SourceCursor {
            unit: 1,
            ..Default::default()
        };
        let saved = state()
            .claim(0)
            .unwrap()
            .candidate(1, "accepted".into(), cursor, false, 1)
            .unwrap()
            .candidate_checked(false)
            .unwrap();
        let first = saved.claim(2).unwrap();
        let second = fail_and_retry(first, FailureKind::InvalidCompletion, 3);
        let third = fail_and_retry(second, FailureKind::InvalidCompletion, 2_004);
        assert_eq!(third.portion_attempts, 3);
        assert_eq!(third.previous_checkpoint.as_deref(), Some("accepted"));
        assert_eq!(third.cursor, cursor);
        assert_eq!(third.retries, 0);
        assert_eq!(third.deadline_ms, saved.deadline_ms);
        let accepted = third
            .candidate(
                third.attempts,
                "next".into(),
                SourceCursor {
                    unit: 2,
                    ..Default::default()
                },
                false,
                10_005,
            )
            .unwrap();
        let next = restart(&accepted)
            .candidate_checked(false)
            .unwrap()
            .claim(10_006)
            .unwrap();
        assert_eq!(next.portion_attempts, 1);
        let next = fail_and_retry(next, FailureKind::InvalidCompletion, 10_007);
        let next = fail_and_retry(next, FailureKind::InvalidCompletion, 12_008);
        assert_eq!(next.portion_attempts, 3);
        assert_eq!(next.previous_checkpoint.as_deref(), Some("next"));
    }

    #[test]
    fn every_mixture_of_retryable_failures_stops_after_three_claims() {
        for mask in 0..8 {
            let mut current = state().claim(0).unwrap();
            for attempt in 0..3 {
                let kind = if mask & (1 << attempt) == 0 {
                    FailureKind::InvalidCompletion
                } else {
                    FailureKind::Transient
                };
                let now = match current.phase {
                    RunnerPhase::Attempt { deadline_ms, .. } => deadline_ms - 1,
                    _ => unreachable!(),
                };
                // Keep all attempts within the end-to-end deadline.
                let now = now.min(20_000 * (attempt + 1));
                let failed = restart(&current)
                    .attempt_failed(kind.clone(), now, None)
                    .unwrap();
                assert_eq!(failed.attempts, attempt + 1);
                assert_eq!(failed.portion_attempts as u64, attempt + 1);
                assert_eq!(failed.cursor, SourceCursor::default());
                assert_eq!(failed.previous_checkpoint, None);
                if attempt == 2 {
                    assert_eq!(failed.phase, RunnerPhase::Failed { kind });
                    assert_eq!(restart(&failed).action(now + 1), RunnerAction::Terminal);
                    assert!(failed.claim(now + 1).is_err());
                } else {
                    let RunnerPhase::Backoff { not_before_ms, .. } = failed.phase else {
                        panic!("retry not admitted");
                    };
                    current = restart(&failed).claim(not_before_ms).unwrap();
                }
            }
        }
    }

    #[test]
    fn transient_limit_remains_operation_wide_after_a_valid_checkpoint() {
        let second = fail_and_retry(state().claim(0).unwrap(), FailureKind::Transient, 1);
        let third = fail_and_retry(second, FailureKind::Transient, 2_002);
        let next = third
            .candidate(
                3,
                "accepted".into(),
                SourceCursor {
                    unit: 1,
                    ..Default::default()
                },
                false,
                10_003,
            )
            .unwrap()
            .candidate_checked(false)
            .unwrap()
            .claim(10_004)
            .unwrap();
        assert_eq!(next.portion_attempts, 1);
        assert_eq!(next.retries, 2);
        assert_eq!(
            next.attempt_failed(FailureKind::Transient, 10_005, None)
                .unwrap()
                .phase,
            RunnerPhase::Failed {
                kind: FailureKind::Transient
            }
        );
        assert!(matches!(
            next.attempt_failed(FailureKind::InvalidCompletion, 10_005, None)
                .unwrap()
                .phase,
            RunnerPhase::Backoff { .. }
        ));
    }

    #[test]
    fn invalid_retry_wait_obeys_deadline_and_stop_and_cannot_reclaim_spent_budget() {
        let first = state().claim(899_000).unwrap();
        let expired = first
            .attempt_failed(FailureKind::InvalidCompletion, 899_001, None)
            .unwrap();
        assert_eq!(
            expired.phase,
            RunnerPhase::Failed {
                kind: FailureKind::Deadline
            }
        );
        let waiting = state()
            .claim(0)
            .unwrap()
            .attempt_failed(FailureKind::InvalidCompletion, 1, None)
            .unwrap();
        assert_eq!(waiting.action(waiting.deadline_ms), RunnerAction::Expired);
        let stopped = restart(&waiting).terminate(FailureKind::Cancelled).unwrap();
        assert_eq!(stopped.action(2_001), RunnerAction::Terminal);
        assert!(stopped.claim(2_001).is_err());
        let mut exhausted = state();
        exhausted.portion_attempts = 3;
        assert!(restart(&exhausted).claim(0).is_err());
    }

    #[test]
    fn invalid_completion_retries_do_not_allow_a_second_correction() {
        let final_candidate = state()
            .claim(0)
            .unwrap()
            .candidate(
                1,
                "final".into(),
                SourceCursor {
                    unit: 1,
                    ..Default::default()
                },
                true,
                1,
            )
            .unwrap();
        let correction = final_candidate.candidate_checked(false).unwrap();
        let cap = correction.target_tokens;
        assert_eq!(correction.corrections, 1);
        let second = fail_and_retry(
            correction.claim(2).unwrap(),
            FailureKind::InvalidCompletion,
            3,
        );
        let third = fail_and_retry(second, FailureKind::InvalidCompletion, 2_004);
        assert_eq!(third.target_tokens, cap);
        assert_eq!(third.corrections, 1);
        let corrected = third
            .candidate(
                third.attempts,
                "corrected".into(),
                third.cursor,
                true,
                10_005,
            )
            .unwrap();
        assert_eq!(
            corrected.candidate_checked(false).unwrap().phase,
            RunnerPhase::Failed {
                kind: FailureKind::InsufficientEffect
            }
        );
    }

    #[test]
    fn only_deadline_resume_renews_exhausted_portion_and_correction_budgets() {
        for purpose in [AttemptPurpose::Portion, AttemptPurpose::Correction] {
            let progress = state()
                .claim(0)
                .unwrap()
                .candidate(
                    1,
                    "accepted".into(),
                    SourceCursor {
                        unit: 1,
                        ..Default::default()
                    },
                    purpose == AttemptPurpose::Correction,
                    1,
                )
                .unwrap()
                .candidate_checked(false)
                .unwrap();
            let second = fail_and_retry(progress.claim(2).unwrap(), FailureKind::Transient, 3);
            let third = fail_and_retry(second, FailureKind::InvalidCompletion, 2_004);
            assert_eq!(third.portion_attempts, 3);
            assert_eq!(restart(&third), third);
            // A process restart within the same execution cannot claim a fourth call.
            let exhausted = restart(&third)
                .attempt_failed(FailureKind::Transient, 10_005, None)
                .unwrap();
            assert_eq!(exhausted.action(10_006), RunnerAction::Terminal);
            assert!(exhausted.claim(10_006).is_err());
            let invalid = third
                .attempt_failed(FailureKind::InvalidCompletion, 10_005, None)
                .unwrap();
            assert!(!invalid.can_resume_deadline());

            let expired = restart(&third).terminate(FailureKind::Deadline).unwrap();
            assert_eq!(expired.portion_attempts, 3);
            assert!(expired.can_resume_deadline());
            let resumed = restart(&expired).resume_deadline(1_800_000, false).unwrap();
            assert_eq!(resumed.portion_attempts, 0);
            assert_eq!(resumed.phase, RunnerPhase::Ready { purpose });
            assert_eq!(resumed.attempts, third.attempts);
            assert_eq!(resumed.retries, third.retries);
            assert_eq!(resumed.corrections, third.corrections);
            assert_eq!(resumed.target_tokens, third.target_tokens);
            assert_eq!(resumed.cursor, third.cursor);
            assert_eq!(resumed.previous_checkpoint, third.previous_checkpoint);
            assert_eq!(
                resumed.source_text_projection_version,
                third.source_text_projection_version
            );
            let attempt = restart(&resumed).claim(900_001).unwrap();
            assert_eq!(attempt.attempts, third.attempts + 1);
            assert_eq!(attempt.portion_attempts, 1);
            let second = fail_and_retry(attempt.clone(), FailureKind::InvalidCompletion, 900_002);
            let third_again = fail_and_retry(second, FailureKind::InvalidCompletion, 902_003);
            assert_eq!(third_again.portion_attempts, 3);
            let exhausted_again = third_again
                .attempt_failed(FailureKind::InvalidCompletion, 910_004, None)
                .unwrap();
            assert_eq!(exhausted_again.action(910_005), RunnerAction::Terminal);
            assert!(exhausted_again.claim(910_005).is_err());
            let cursor = if purpose == AttemptPurpose::Correction {
                third.cursor
            } else {
                SourceCursor {
                    unit: 2,
                    ..Default::default()
                }
            };
            let candidate = attempt
                .candidate(attempt.attempts, "resumed".into(), cursor, true, 900_002)
                .unwrap();
            assert!(matches!(
                candidate.candidate_checked(true).unwrap().phase,
                RunnerPhase::Commit { .. }
            ));
            if purpose == AttemptPurpose::Correction {
                assert_eq!(
                    candidate.candidate_checked(false).unwrap().phase,
                    RunnerPhase::Failed {
                        kind: FailureKind::InsufficientEffect
                    }
                );
            }
        }
    }

    #[test]
    fn deadline_resume_keeps_legacy_backoff_and_operation_wide_limits() {
        let legacy = state()
            .claim(0)
            .unwrap()
            .candidate(
                1,
                "accepted".into(),
                SourceCursor {
                    unit: 1,
                    ..Default::default()
                },
                false,
                1,
            )
            .unwrap()
            .candidate_checked(false)
            .unwrap()
            .claim(2)
            .unwrap()
            .attempt_failed(FailureKind::Transient, 3, Some(700_000))
            .unwrap();
        let mut json =
            serde_json::to_value(legacy.terminate(FailureKind::Deadline).unwrap()).unwrap();
        json.as_object_mut().unwrap().remove("portion_attempts");
        let expired: RunnerState = serde_json::from_value(json).unwrap();
        let resumed = expired.resume_deadline(1_800_000, false).unwrap();
        assert_eq!(resumed.phase, legacy.phase);
        assert_eq!(resumed.action(300_001), RunnerAction::WaitUntil(700_003));
        assert!(resumed.claim(700_002).is_err());
        let attempt = restart(&resumed).claim(700_003).unwrap();
        assert_eq!(attempt.retries, 1);
        // Missing legacy counters still use their conservative transient accounting.
        assert_eq!(attempt.portion_attempts, 2);
    }

    #[test]
    fn legacy_json_without_portion_counter_is_readable_and_interrupted_call_is_counted() {
        for original in [state(), state().claim(0).unwrap()] {
            let mut json = serde_json::to_value(&original).unwrap();
            json.as_object_mut().unwrap().remove("portion_attempts");
            let restored: RunnerState = serde_json::from_value(json).unwrap();
            assert_eq!(restored.portion_attempts, 0);
            if matches!(restored.phase, RunnerPhase::Attempt { .. }) {
                let next = fail_and_retry(restored, FailureKind::Transient, 1);
                assert_eq!(next.portion_attempts, 2);
            } else {
                assert_eq!(restored.claim(0).unwrap().portion_attempts, 1);
            }
        }
    }

    #[test]
    fn legacy_inflight_and_backoff_records_do_not_gain_calls_on_upgrade() {
        let second = fail_and_retry(state().claim(0).unwrap(), FailureKind::Transient, 1);
        let waiting = second
            .attempt_failed(FailureKind::Transient, 2_002, None)
            .unwrap();
        let RunnerPhase::Backoff { not_before_ms, .. } = waiting.phase else {
            unreachable!()
        };
        for original in [waiting.clone(), waiting.claim(not_before_ms).unwrap()] {
            let mut json = serde_json::to_value(&original).unwrap();
            json.as_object_mut().unwrap().remove("portion_attempts");
            let restored: RunnerState = serde_json::from_value(json).unwrap();
            let third = if matches!(restored.phase, RunnerPhase::Backoff { .. }) {
                restored.claim(not_before_ms).unwrap()
            } else {
                restored
            };
            let failed = third
                .attempt_failed(FailureKind::InvalidCompletion, not_before_ms + 1, None)
                .unwrap();
            assert_eq!(failed.portion_attempts, 3);
            assert_eq!(
                failed.phase,
                RunnerPhase::Failed {
                    kind: FailureKind::InvalidCompletion
                }
            );
        }
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
