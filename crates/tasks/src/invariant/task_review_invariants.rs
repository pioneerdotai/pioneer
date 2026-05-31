use anyhow::Result;
use pioneer_crud::{CrudStore, TaskReviewInvariantSnapshot};
use pioneer_protocol::{TaskAgentToolPolicy, TaskAgentWriteMode};
use std::collections::{BTreeMap, HashMap, HashSet};

use super::{
    TaskRuntimeInvariantReport, TaskRuntimeInvariantViolation, TaskRuntimeInvariantViolationKind,
};

pub(super) async fn detect_task_review_violations(
    store: &CrudStore,
    observed_at_unix: i64,
    report: &mut TaskRuntimeInvariantReport,
) -> Result<()> {
    let Some(snapshot) = store.load_task_review_invariant_snapshot().await? else {
        return Ok(());
    };

    detect_primary_executor_binding_violations(&snapshot, report);
    detect_task_run_turn_reference_violations(&snapshot, report);
    detect_accepted_candidate_violations(&snapshot, report);
    detect_waiting_review_write_lock_violations(&snapshot, observed_at_unix, report);
    detect_target_model_violations(&snapshot, report);
    Ok(())
}

fn detect_primary_executor_binding_violations(
    snapshot: &TaskReviewInvariantSnapshot,
    report: &mut TaskRuntimeInvariantReport,
) {
    let lineage_threads = snapshot
        .thread_lineage
        .iter()
        .map(|lineage| lineage.child_thread_id.as_str())
        .collect::<HashSet<_>>();

    let mut bindings = snapshot.primary_bindings.iter().collect::<Vec<_>>();
    bindings.sort_by(|left, right| left.id.cmp(&right.id));

    for binding in &bindings {
        if !lineage_threads.contains(binding.thread_id.as_str()) {
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::PrimaryExecutorBindingMissingLineage {
                    task_id: binding.task_id.clone(),
                    run_id: binding.run_id.clone(),
                    binding_id: binding.id.clone(),
                    thread_id: binding.thread_id.clone(),
                },
                "primary_executor task_run_thread_binding.thread_id has no thread_lineage graph row",
            ));
        }
        if binding.execution_id.is_none() {
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::PrimaryExecutorBindingMissingExecution {
                    task_id: binding.task_id.clone(),
                    run_id: binding.run_id.clone(),
                    binding_id: binding.id.clone(),
                    thread_id: binding.thread_id.clone(),
                },
                "primary_executor task_run_thread_binding has no execution_id",
            ));
        }
    }

    let mut bindings_by_run: BTreeMap<&str, Vec<_>> = BTreeMap::new();
    for binding in bindings {
        bindings_by_run
            .entry(binding.run_id.as_str())
            .or_default()
            .push(binding);
    }
    for (run_id, bindings) in bindings_by_run {
        if bindings.len() < 2 {
            continue;
        }
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::MultiplePrimaryExecutorBindingsForRun {
                task_id: bindings[0].task_id.clone(),
                run_id: run_id.to_owned(),
                binding_ids: bindings.iter().map(|binding| binding.id.clone()).collect(),
            },
            "one task run has multiple primary_executor task_run_thread_binding rows",
        ));
    }
}

fn detect_task_run_turn_reference_violations(
    snapshot: &TaskReviewInvariantSnapshot,
    report: &mut TaskRuntimeInvariantReport,
) {
    let existing_turn_ids = snapshot
        .turn_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let lineage_threads = snapshot
        .thread_lineage
        .iter()
        .map(|lineage| lineage.child_thread_id.as_str())
        .collect::<HashSet<_>>();

    let mut turns = snapshot.task_run_turns.iter().collect::<Vec<_>>();
    turns.sort_by(|left, right| {
        left.run_id
            .cmp(&right.run_id)
            .then_with(|| left.sequence.cmp(&right.sequence))
            .then_with(|| left.id.cmp(&right.id))
    });

    for turn in turns {
        if !existing_turn_ids.contains(turn.turn_id.as_str()) {
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::TaskRunTurnMissingTurn {
                    task_id: turn.task_id.clone(),
                    run_id: turn.run_id.clone(),
                    task_run_turn_id: turn.id.clone(),
                    turn_id: turn.turn_id.clone(),
                },
                "task_run_turn.turn_id does not reference an existing turn row",
            ));
        }
        if !lineage_threads.contains(turn.thread_id.as_str()) {
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::TaskRunTurnMissingLineage {
                    task_id: turn.task_id.clone(),
                    run_id: turn.run_id.clone(),
                    task_run_turn_id: turn.id.clone(),
                    thread_id: turn.thread_id.clone(),
                    turn_id: turn.turn_id.clone(),
                },
                "task_run_turn.thread_id has no thread_lineage graph row",
            ));
        }
    }
}

fn detect_accepted_candidate_violations(
    snapshot: &TaskReviewInvariantSnapshot,
    report: &mut TaskRuntimeInvariantReport,
) {
    let turns_by_id = snapshot
        .task_run_turns
        .iter()
        .map(|turn| (turn.id.as_str(), turn))
        .collect::<HashMap<_, _>>();
    let reviews_by_id = snapshot
        .task_result_review_events
        .iter()
        .map(|review| (review.id.as_str(), review))
        .collect::<HashMap<_, _>>();

    let mut candidates = snapshot.task_result_candidates.iter().collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.id.cmp(&right.id));

    let mut accepted_by_run: BTreeMap<(&str, &str), Vec<_>> = BTreeMap::new();
    for candidate in &candidates {
        if candidate.status == "accepted" {
            accepted_by_run
                .entry((candidate.task_id.as_str(), candidate.run_id.as_str()))
                .or_default()
                .push(*candidate);

            if !turns_by_id.contains_key(candidate.task_run_turn_id.as_str()) {
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::AcceptedCandidateMissingTurn {
                        task_id: candidate.task_id.clone(),
                        run_id: candidate.run_id.clone(),
                        candidate_id: candidate.id.clone(),
                        task_run_turn_id: candidate.task_run_turn_id.clone(),
                    },
                    "accepted task_result_candidate does not belong to an existing task_run_turn",
                ));
            }
            if candidate.result_json.is_none() {
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::AcceptedCandidateMissingResult {
                        task_id: candidate.task_id.clone(),
                        run_id: candidate.run_id.clone(),
                        candidate_id: candidate.id.clone(),
                    },
                    "accepted task_result_candidate has no result_json",
                ));
            }
            let final_review = candidate
                .final_review_event_id
                .as_deref()
                .and_then(|review_id| reviews_by_id.get(review_id).copied())
                .filter(|review| review.candidate_id == candidate.id);
            if final_review.is_none() {
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::AcceptedCandidateMissingFinalReviewEvent {
                        task_id: candidate.task_id.clone(),
                        run_id: candidate.run_id.clone(),
                        candidate_id: candidate.id.clone(),
                        final_review_event_id: candidate.final_review_event_id.clone(),
                    },
                    "accepted task_result_candidate has no matching final review event",
                ));
            }
        }
    }

    let runs_with_turns = snapshot
        .task_run_turns
        .iter()
        .map(|turn| turn.run_id.as_str())
        .collect::<HashSet<_>>();
    for run in &snapshot.task_runs {
        if run.status == "succeeded"
            && run.result_json.is_some()
            && runs_with_turns.contains(run.id.as_str())
            && !accepted_by_run.contains_key(&(run.task_id.as_str(), run.id.as_str()))
        {
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::SucceededRunMissingAcceptedCandidate {
                    task_id: run.task_id.clone(),
                    run_id: run.id.clone(),
                },
                "succeeded task_run with result_json has no accepted task_result_candidate",
            ));
        }
    }

    for ((task_id, run_id), candidates) in accepted_by_run {
        if candidates.len() < 2 {
            continue;
        }
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::MultipleAcceptedCandidatesForRun {
                task_id: task_id.to_owned(),
                run_id: run_id.to_owned(),
                candidate_ids: candidates
                    .iter()
                    .map(|candidate| candidate.id.clone())
                    .collect(),
            },
            "one task run has multiple accepted task_result_candidate rows",
        ));
    }
}

fn detect_target_model_violations(
    snapshot: &TaskReviewInvariantSnapshot,
    report: &mut TaskRuntimeInvariantReport,
) {
    detect_candidate_turn_mismatches(snapshot, report);
    detect_review_reference_violations(snapshot, report);
    detect_task_run_turn_order_violations(snapshot, report);
}

fn detect_waiting_review_write_lock_violations(
    snapshot: &TaskReviewInvariantSnapshot,
    observed_at_unix: i64,
    report: &mut TaskRuntimeInvariantReport,
) {
    for run in snapshot
        .task_runs
        .iter()
        .filter(|run| run.status == "waiting_review")
    {
        if !run_has_scoped_write_policy(snapshot, run.task_id.as_str(), run.id.as_str()) {
            continue;
        }
        let has_active_lock = snapshot.write_locks.iter().any(|lock| {
            lock.run_id == run.id
                && lock.status == "acquired"
                && lock
                    .expires_at_unix
                    .is_none_or(|expires_at| expires_at > observed_at_unix)
        });
        if has_active_lock {
            continue;
        }
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::WaitingReviewRunMissingActiveWriteLock {
                task_id: run.task_id.clone(),
                run_id: run.id.clone(),
            },
            "waiting_review task_run with scoped write policy has no active write lock",
        ));
    }
}

fn run_has_scoped_write_policy(
    snapshot: &TaskReviewInvariantSnapshot,
    task_id: &str,
    run_id: &str,
) -> bool {
    snapshot.agent_specs.iter().any(|spec| {
        spec.task_id == task_id
            && (spec.run_id.as_deref() == Some(run_id) || spec.run_id.is_none())
            && spec
                .tool_policy_json
                .as_deref()
                .is_some_and(tool_policy_is_scoped_write)
    })
}

fn tool_policy_is_scoped_write(value: &str) -> bool {
    serde_json::from_str::<TaskAgentToolPolicy>(value)
        .is_ok_and(|policy| policy.write_mode == TaskAgentWriteMode::ScopedWrite)
}

fn detect_candidate_turn_mismatches(
    snapshot: &TaskReviewInvariantSnapshot,
    report: &mut TaskRuntimeInvariantReport,
) {
    let turns_by_id = snapshot
        .task_run_turns
        .iter()
        .map(|turn| (turn.id.as_str(), turn))
        .collect::<HashMap<_, _>>();
    let mut primary_bindings_by_run: BTreeMap<&str, Vec<_>> = BTreeMap::new();
    for binding in &snapshot.primary_bindings {
        primary_bindings_by_run
            .entry(binding.run_id.as_str())
            .or_default()
            .push(binding);
    }

    let mut candidates = snapshot.task_result_candidates.iter().collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.id.cmp(&right.id));

    for candidate in candidates {
        if let Some(turn) = turns_by_id
            .get(candidate.task_run_turn_id.as_str())
            .copied()
        {
            if candidate.thread_id != turn.thread_id || candidate.turn_id != turn.turn_id {
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::TaskResultCandidateTurnMismatch {
                        task_id: candidate.task_id.clone(),
                        run_id: candidate.run_id.clone(),
                        candidate_id: candidate.id.clone(),
                        task_run_turn_id: candidate.task_run_turn_id.clone(),
                        candidate_thread_id: candidate.thread_id.clone(),
                        turn_thread_id: turn.thread_id.clone(),
                        candidate_turn_id: candidate.turn_id.clone(),
                        turn_turn_id: turn.turn_id.clone(),
                    },
                    "task_result_candidate thread/turn ids do not match its task_run_turn",
                ));
            }
            if candidate.round != turn.round {
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::TaskResultCandidateRoundMismatch {
                        task_id: candidate.task_id.clone(),
                        run_id: candidate.run_id.clone(),
                        candidate_id: candidate.id.clone(),
                        task_run_turn_id: candidate.task_run_turn_id.clone(),
                        candidate_round: candidate.round,
                        turn_round: turn.round,
                    },
                    "task_result_candidate round does not match its task_run_turn round",
                ));
            }
        }

        if let Some(bindings) = primary_bindings_by_run.get(candidate.run_id.as_str()) {
            for binding in bindings {
                if candidate.thread_id == binding.thread_id {
                    continue;
                }
                report.push(TaskRuntimeInvariantViolation::new(
                    TaskRuntimeInvariantViolationKind::TaskResultCandidatePrimaryBindingMismatch {
                        task_id: candidate.task_id.clone(),
                        run_id: candidate.run_id.clone(),
                        candidate_id: candidate.id.clone(),
                        binding_id: binding.id.clone(),
                        candidate_thread_id: candidate.thread_id.clone(),
                        binding_thread_id: binding.thread_id.clone(),
                    },
                    "task_result_candidate thread_id does not match the run primary_executor binding",
                ));
            }
        }
    }
}

fn detect_review_reference_violations(
    snapshot: &TaskReviewInvariantSnapshot,
    report: &mut TaskRuntimeInvariantReport,
) {
    let candidates_by_id = snapshot
        .task_result_candidates
        .iter()
        .map(|candidate| (candidate.id.as_str(), candidate))
        .collect::<HashMap<_, _>>();
    let turns_by_id = snapshot
        .task_run_turns
        .iter()
        .map(|turn| (turn.id.as_str(), turn))
        .collect::<HashMap<_, _>>();
    let reviews_by_id = snapshot
        .task_result_review_events
        .iter()
        .map(|review| (review.id.as_str(), review))
        .collect::<HashMap<_, _>>();

    let mut reviews = snapshot
        .task_result_review_events
        .iter()
        .collect::<Vec<_>>();
    reviews.sort_by(|left, right| left.id.cmp(&right.id));

    for review in reviews {
        if !candidates_by_id.contains_key(review.candidate_id.as_str()) {
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::ReviewEventMissingCandidate {
                    task_id: review.task_id.clone(),
                    run_id: review.run_id.clone(),
                    review_event_id: review.id.clone(),
                    candidate_id: review.candidate_id.clone(),
                },
                "task_result_review_event.candidate_id does not reference a candidate",
            ));
        }
        if !review.task_run_turn_id.is_empty()
            && !turns_by_id.contains_key(review.task_run_turn_id.as_str())
        {
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::ReviewEventMissingTaskRunTurn {
                    task_id: review.task_id.clone(),
                    run_id: review.run_id.clone(),
                    review_event_id: review.id.clone(),
                    task_run_turn_id: review.task_run_turn_id.clone(),
                },
                "task_result_review_event.task_run_turn_id does not reference a task_run_turn",
            ));
        }
    }

    for candidate in &snapshot.task_result_candidates {
        let Some(review_id) = candidate.final_review_event_id.as_deref() else {
            continue;
        };
        let Some(review) = reviews_by_id.get(review_id).copied() else {
            continue;
        };
        let mismatch = match candidate.status.as_str() {
            "accepted" => review.decision != "accept",
            "rejected" => review.decision != "request_changes" && review.decision != "reject",
            "cancelled" => review.decision != "cancel",
            _ => false,
        };
        if mismatch {
            report.push(TaskRuntimeInvariantViolation::new(
                TaskRuntimeInvariantViolationKind::FinalReviewEventDecisionMismatch {
                    task_id: candidate.task_id.clone(),
                    run_id: candidate.run_id.clone(),
                    candidate_id: candidate.id.clone(),
                    review_event_id: review.id.clone(),
                    candidate_status: candidate.status.clone(),
                    review_decision: review.decision.clone(),
                },
                "task_result_candidate final review decision does not match candidate status",
            ));
        }
    }
}

fn detect_task_run_turn_order_violations(
    snapshot: &TaskReviewInvariantSnapshot,
    report: &mut TaskRuntimeInvariantReport,
) {
    let mut by_sequence: BTreeMap<(&str, i64), Vec<_>> = BTreeMap::new();
    let mut by_candidate_round: BTreeMap<(&str, i64), Vec<_>> = BTreeMap::new();

    for turn in &snapshot.task_run_turns {
        by_sequence
            .entry((turn.run_id.as_str(), turn.sequence))
            .or_default()
            .push(turn);
        if matches!(turn.kind.as_str(), "initial" | "revision" | "recovery") {
            by_candidate_round
                .entry((turn.run_id.as_str(), turn.round))
                .or_default()
                .push(turn);
        }
    }

    for ((run_id, sequence), turns) in by_sequence {
        if turns.len() < 2 {
            continue;
        }
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::DuplicateTaskRunTurnSequence {
                task_id: turns[0].task_id.clone(),
                run_id: run_id.to_owned(),
                sequence,
                task_run_turn_ids: turns.iter().map(|turn| turn.id.clone()).collect(),
            },
            "one task run has multiple task_run_turn rows with the same sequence",
        ));
    }

    for ((run_id, round), turns) in by_candidate_round {
        if turns.len() < 2 {
            continue;
        }
        report.push(TaskRuntimeInvariantViolation::new(
            TaskRuntimeInvariantViolationKind::DuplicateCandidateProducingRound {
                task_id: turns[0].task_id.clone(),
                run_id: run_id.to_owned(),
                round,
                task_run_turn_ids: turns.iter().map(|turn| turn.id.clone()).collect(),
            },
            "one task run has multiple candidate-producing task_run_turn rows with the same round",
        ));
    }
}
