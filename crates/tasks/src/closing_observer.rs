//! Diagnostic sidecar owned by TaskEventBus. Only committed events and existing
//! recovery reads feed it; it never discovers, schedules or authorizes work.
use pioneer_observability::{
    closing::{Closing, Fact, Loss, Phase},
    live::WaitKind,
    scenario::Correlation,
};
use pioneer_protocol::{
    TaskDelivery, TaskDeliveryStatus, TaskEventPayload, TaskResultCandidate,
    TaskResultCandidateStatus,
};
use std::collections::VecDeque;

// Two bounded slices of the existing 512-entry live budget. Ended causal refs
// share the same caps as waits. FIFO eviction censors, never completes work.
const SLOTS: usize = 64;
const ID_BYTES: usize = 256;
struct Run {
    task: String,
    run: String,
    candidate: String,
    turn: String,
    round: u32,
    generation: Option<u64>,
    accepted: bool,
    sequence: i64,
    parent: Correlation,
    wait: Option<Closing>,
    result: Option<Closing>,
    result_fact: Option<Fact>,
    result_observed: bool,
    reviews: [Correlation; 4],
    review_count: usize,
    reviews_overflow: bool,
}
struct Delivery {
    id: String,
    run: String,
    attempt: u32,
    sequence: i64,
    parent: Correlation,
    wait: Option<Closing>,
    episode: Option<Closing>,
}
#[derive(Default)]
pub(crate) struct ClosingObserver {
    runs: VecDeque<Run>,
    deliveries: VecDeque<Delivery>,
    publications: VecDeque<(String, i64, Correlation)>,
    retired: bool,
    lifecycle: u64,
    admissions: VecDeque<(String, String, i64, u64, bool)>,
    terminal_runs: VecDeque<(String, i64, Fact)>,
    terminal_tasks: VecDeque<(String, i64)>,
    // Lost historical evidence requires a fresh accepted lifecycle receipt.
    // It never disables independently admitted new work.
    review_coverage_lost: bool,
}
fn ids_fit(ids: &[&str]) -> bool {
    let fits = ids.iter().all(|id| id.len() <= ID_BYTES);
    if !fits {
        Closing::loss(Loss::IdentityCapacity);
    }
    fits
}
fn finish(wait: &mut Option<Closing>, parent: &mut Correlation, fact: Fact) {
    if let Some(mut wait) = wait.take() {
        *parent = wait.correlation();
        wait.finish(fact);
    }
}
fn end_result(slot: &mut Run, fact: Fact) {
    let fact = *slot.result_fact.get_or_insert(fact);
    if let Some(mut result) = slot.result.take() {
        result.finish(fact);
        slot.result_observed = true;
    }
}
impl ClosingObserver {
    pub fn ticket(&self) -> u64 {
        self.lifecycle
    }
    pub fn generation(&self, run: &str) -> Option<u64> {
        self.admissions
            .iter()
            .find(|(_, id, _, _, _)| id == run)
            .map(|(_, _, _, generation, _)| *generation)
    }
    pub fn admit(&mut self, ticket: u64, task: &str, run: &str, sequence: i64, resume: bool) {
        if self.retired
            || !pioneer_observability::telemetry_enabled()
            || sequence <= 0
            || self.lifecycle == u64::MAX
        {
            return;
        }
        if ticket != self.lifecycle {
            Closing::loss(Loss::RecoveredFragment);
            return;
        }
        if !ids_fit(&[task, run]) {
            return;
        }
        if let Some((_, _, floor, _, _)) = self.admissions.iter().find(|(_, id, _, _, _)| id == run)
        {
            if !resume || sequence <= *floor {
                return;
            }
        }
        if self
            .terminal_runs
            .iter()
            .any(|(id, seq, _)| id == run && *seq >= sequence)
            || self
                .terminal_tasks
                .iter()
                .any(|(id, seq)| id == task && *seq >= sequence)
        {
            return;
        }
        if !resume && self.terminal_runs.iter().any(|(id, _, _)| id == run) {
            return;
        }
        if resume {
            self.runs.retain(|slot| slot.run != run);
            self.terminal_runs.retain(|(id, _, _)| id != run);
        }
        self.admissions.retain(|(_, id, _, _, _)| id != run);
        if self.admissions.len() == SLOTS {
            self.admissions.pop_front();
            self.review_coverage_lost = true;
            Closing::loss(Loss::OwnerCapacity);
        }
        self.lifecycle = self.lifecycle.saturating_add(1);
        self.admissions.push_back((
            task.to_owned(),
            run.to_owned(),
            sequence,
            self.lifecycle,
            false,
        ));
    }
    fn review_fenced(&self, task: &str, run: &str, sequence: Option<i64>) -> bool {
        let admission = self
            .admissions
            .iter()
            .find(|(owner, id, _, _, _)| owner == task && id == run);
        if let Some((_, _, floor, _, closed)) = admission {
            if *closed {
                return true;
            }
            if sequence.is_none_or(|seq| seq <= *floor) {
                return true;
            }
            return self
                .terminal_runs
                .iter()
                .any(|(id, seq, _)| id == run && seq > floor)
                || self
                    .terminal_tasks
                    .iter()
                    .any(|(id, seq)| id == task && seq > floor);
        }
        self.review_coverage_lost
            || self.terminal_runs.iter().any(|(id, _, _)| id == run)
            || self.terminal_tasks.iter().any(|(id, _)| id == task)
    }
    fn remember_run_terminal(&mut self, id: &str, sequence: i64, fact: Fact) {
        if self.terminal_runs.iter().any(|(key, _, _)| key == id) {
            return;
        }
        if !ids_fit(&[id]) {
            self.review_coverage_lost = true;
            return;
        }
        if self.terminal_runs.len() == SLOTS {
            self.terminal_runs.pop_front();
            self.review_coverage_lost = true;
            Closing::loss(Loss::OwnerCapacity);
        }
        self.terminal_runs
            .push_back((id.to_owned(), sequence, fact));
    }
    fn remember_task_terminal(&mut self, id: &str, sequence: i64) {
        if self.terminal_tasks.iter().any(|(key, _)| key == id) {
            return;
        }
        if !ids_fit(&[id]) {
            self.review_coverage_lost = true;
            return;
        }
        if self.terminal_tasks.len() == SLOTS {
            self.terminal_tasks.pop_front();
            self.review_coverage_lost = true;
            Closing::loss(Loss::OwnerCapacity);
        }
        self.terminal_tasks.push_back((id.to_owned(), sequence));
    }
    pub fn handoff_result(
        &mut self,
        run: &str,
        turn: &str,
        mut result: Closing,
        generation: Option<u64>,
    ) {
        if self.retired {
            return;
        }
        if let Some(slot) = self.runs.iter_mut().find(|slot| {
            slot.run == run
                && slot.turn == turn
                && !slot.candidate.is_empty()
                && slot.generation == generation
        }) {
            if slot.result_observed || slot.result.is_some() {
                result.finish(Fact::Duplicate);
            } else if let Some(fact) = slot.result_fact {
                result.finish(fact);
                slot.result_observed = true;
            } else {
                slot.result = Some(result);
            }
        }
        // No matching durable owner or eviction: Drop censors the source.
    }
    pub fn observes(event: &TaskEventPayload) -> bool {
        matches!(
            event,
            TaskEventPayload::TaskResultCandidateCreated { .. }
                | TaskEventPayload::TaskResultReviewEventRecorded { .. }
                | TaskEventPayload::TaskRunEnteredReview { .. }
                | TaskEventPayload::TaskResultCandidateAccepted { .. }
                | TaskEventPayload::TaskResultCandidateRejected { .. }
                | TaskEventPayload::TaskResultCandidateCancelled { .. }
                | TaskEventPayload::RunCompleted { .. }
                | TaskEventPayload::RunFailed { .. }
                | TaskEventPayload::RunCancelled { .. }
                | TaskEventPayload::RunBlocked { .. }
                | TaskEventPayload::TaskRevisionRequested { .. }
                | TaskEventPayload::DeliveryQueued { .. }
                | TaskEventPayload::DeliveryStarted { .. }
                | TaskEventPayload::DeliveryDelivered { .. }
                | TaskEventPayload::DeliveryFailed { .. }
                | TaskEventPayload::DeliveryCancelled { .. }
                | TaskEventPayload::TaskCancelled { .. }
                | TaskEventPayload::TaskCompleted { .. }
                | TaskEventPayload::TaskFailed { .. }
        )
    }
    fn selected_run(&self, run: &str) -> Option<&Run> {
        let mut accepted = self
            .runs
            .iter()
            .filter(|slot| slot.run == run && slot.accepted);
        if let Some(slot) = accepted.next() {
            return accepted.next().is_none().then_some(slot);
        }
        let mut candidates = self.runs.iter().filter(|slot| slot.run == run);
        let slot = candidates.next()?;
        candidates.next().is_none().then_some(slot)
    }
    pub fn run_parent(&self, run: &str) -> Correlation {
        self.selected_run(run)
            .map(|slot| Closing::continue_from(&slot.parent))
            .unwrap_or_else(Closing::capture)
    }
    pub fn run_links(&self, run: &str) -> ([Correlation; 5], usize) {
        let mut links = std::array::from_fn(|_| Correlation::default());
        if self.selected_run(run).is_some() {
            return (links, 0);
        }
        let mut count = 0;
        for slot in self.runs.iter().filter(|slot| slot.run == run).take(5) {
            if count < 4 {
                links[count] = slot.parent.clone();
            }
            count += 1;
        }
        (links, count)
    }
    pub fn candidate_parent(&self, run: &str, candidate: &str) -> Correlation {
        self.runs
            .iter()
            .find(|slot| slot.run == run && slot.candidate == candidate)
            .map(|slot| Closing::continue_from(&slot.parent))
            .unwrap_or_else(Closing::capture)
    }
    pub fn review_links(
        &self,
        run: &str,
        candidate: &str,
        source: Correlation,
    ) -> ([Correlation; 5], usize) {
        let mut links = std::array::from_fn(|_| Correlation::default());
        links[0] = source;
        let mut count = 1;
        if let Some(slot) = self
            .runs
            .iter()
            .find(|slot| slot.run == run && slot.candidate == candidate)
        {
            for link in slot.reviews.iter().take(slot.review_count.min(3)) {
                links[count] = link.clone();
                count += 1;
            }
            if slot.review_count > 3 || slot.reviews_overflow {
                count = 5;
            }
        }
        (links, count)
    }
    pub fn delivery_parent(&self, id: &str, attempt: u32) -> Correlation {
        self.deliveries
            .iter()
            .find(|slot| slot.id == id && slot.attempt == attempt)
            .map(|slot| Closing::continue_from(&slot.parent))
            .unwrap_or_else(Closing::capture)
    }
    // Exact committed publication references only. They are not run/candidate
    // authority and survive no longer than this existing bounded sidecar.
    pub fn publication(&mut self, task: &str, sequence: i64, parent: Correlation) {
        if self.retired || !ids_fit(&[task]) || sequence <= 0 {
            return;
        }
        if self
            .publications
            .iter()
            .any(|(id, seq, _)| id == task && *seq == sequence)
        {
            return;
        }
        if self.publications.len() == SLOTS {
            self.publications.pop_front();
            Closing::loss(Loss::OwnerCapacity);
        }
        self.publications
            .push_back((task.to_owned(), sequence, parent));
    }
    pub fn publication_parent(&self, task: &str, sequence: i64) -> Option<Correlation> {
        self.publications
            .iter()
            .find(|(id, seq, _)| id == task && *seq == sequence)
            .map(|(_, _, parent)| parent.clone())
    }
    pub fn retire(&mut self) {
        self.retired = true;
        self.publications.clear();
        self.runs.clear();
        self.deliveries.clear();
    }
    pub fn recover_review(
        &mut self,
        candidate: &TaskResultCandidate,
        run: &pioneer_protocol::TaskRun,
    ) {
        if self.review_fenced(&candidate.task_id, &candidate.run_id, None)
            || run.status != pioneer_protocol::TaskRunStatus::WaitingReview
            || run.id != candidate.run_id
            || run.task_id != candidate.task_id
        {
            return;
        }
        if self.retired
            || !pioneer_observability::telemetry_enabled()
            || self
                .runs
                .iter()
                .any(|slot| slot.run == candidate.run_id && slot.candidate == candidate.id)
        {
            return;
        }
        if !matches!(
            candidate.status,
            TaskResultCandidateStatus::PendingReview | TaskResultCandidateStatus::ExtractionFailed
        ) {
            return;
        }
        // Current recovery callback provenance, never reconstructed from IDs.
        self.candidate(candidate, 0, Closing::capture());
        if let Some(slot) = self
            .runs
            .iter_mut()
            .find(|slot| slot.run == candidate.run_id && slot.candidate == candidate.id)
        {
            let mut wait = Closing::start_recovered(Phase::ReviewWait, &slot.parent);
            wait.wait(WaitKind::UserOrProvider);
            slot.parent = wait.correlation();
            slot.wait = Some(wait);
            Closing::loss(Loss::RecoveredFragment);
        }
    }
    fn candidate(&mut self, candidate: &TaskResultCandidate, sequence: i64, parent: Correlation) {
        if self.review_fenced(&candidate.task_id, &candidate.run_id, Some(sequence)) {
            return;
        }
        if !ids_fit(&[
            &candidate.id,
            &candidate.task_id,
            &candidate.run_id,
            &candidate.task_run_turn_id,
        ]) {
            return;
        }
        if let Some(index) = self
            .runs
            .iter()
            .position(|slot| slot.run == candidate.run_id && slot.candidate == candidate.id)
        {
            if self.runs[index].sequence >= sequence {
                return;
            }
            let mut old = self.runs.remove(index).expect("located closing run");
            finish(&mut old.wait, &mut old.parent, Fact::Unconfirmed);
        } else if self.runs.len() == SLOTS {
            self.runs.pop_front();
            Closing::loss(Loss::OwnerCapacity);
        }
        let generation = self.generation(&candidate.run_id);
        self.runs.push_back(Run {
            task: candidate.task_id.clone(),
            run: candidate.run_id.clone(),
            candidate: candidate.id.clone(),
            turn: candidate.task_run_turn_id.clone(),
            round: candidate.round,
            generation,
            accepted: candidate.status == TaskResultCandidateStatus::Accepted,
            sequence,
            parent,
            wait: None,
            result: None,
            result_fact: None,
            result_observed: false,
            reviews: std::array::from_fn(|_| Correlation::default()),
            review_count: 0,
            reviews_overflow: false,
        });
    }
    pub fn recover_delivery(&mut self, delivery: &TaskDelivery) {
        if self.retired
            || !pioneer_observability::telemetry_enabled()
            || self.deliveries.iter().any(|slot| slot.id == delivery.id)
        {
            return;
        }
        if delivery.status == TaskDeliveryStatus::Pending {
            self.delivery(delivery, 0, Fact::Scheduled, None);
            Closing::loss(Loss::RecoveredFragment);
        }
    }
    fn delivery(
        &mut self,
        delivery: &TaskDelivery,
        sequence: i64,
        fact: Fact,
        source: Option<Correlation>,
    ) {
        if !ids_fit(&[&delivery.id, &delivery.run_id]) {
            return;
        }
        let index = self
            .deliveries
            .iter()
            .position(|slot| slot.id == delivery.id);
        if let Some(index) = index {
            let slot = &mut self.deliveries[index];
            if slot.run != delivery.run_id
                || sequence <= slot.sequence
                || delivery.attempt_count < slot.attempt
            {
                return;
            }
            slot.sequence = sequence;
            slot.attempt = delivery.attempt_count;
            finish(&mut slot.wait, &mut slot.parent, fact);
            if matches!(
                delivery.status,
                TaskDeliveryStatus::Delivered
                    | TaskDeliveryStatus::Failed
                    | TaskDeliveryStatus::Cancelled
            ) {
                if let Some(mut episode) = slot.episode.take() {
                    episode.finish(
                        if delivery.mode == pioneer_protocol::TaskDeliveryMode::None {
                            Fact::Returned
                        } else {
                            fact
                        },
                    );
                }
            }
            if let Some(source) = source {
                slot.parent = source;
            }
            if delivery.status == TaskDeliveryStatus::Pending {
                let mut wait = Closing::start(Phase::PendingDelivery, &slot.parent);
                wait.wait(WaitKind::Timer);
                slot.parent = wait.correlation();
                slot.wait = Some(wait);
            }
            return;
        }
        if self.deliveries.len() == SLOTS {
            self.deliveries.pop_front();
            Closing::loss(Loss::OwnerCapacity);
        }
        let parent = self.run_parent(&delivery.run_id);
        let terminal = matches!(
            delivery.status,
            TaskDeliveryStatus::Delivered
                | TaskDeliveryStatus::Failed
                | TaskDeliveryStatus::Cancelled
        );
        let episode = if terminal {
            Closing::loss(Loss::RecoveredFragment);
            None
        } else {
            Some(if sequence == 0 || !matches!(fact, Fact::Scheduled) {
                Closing::loss(Loss::RecoveredFragment);
                Closing::start_recovered(Phase::DestinationClosing, &parent)
            } else {
                Closing::start(Phase::DestinationClosing, &parent)
            })
        };
        let mut parent = episode.as_ref().map(Closing::correlation).unwrap_or(parent);
        let wait = if delivery.status == TaskDeliveryStatus::Pending {
            let mut wait = Closing::start(Phase::PendingDelivery, &parent);
            wait.wait(WaitKind::Timer);
            parent = wait.correlation();
            Some(wait)
        } else {
            None
        };
        self.deliveries.push_back(Delivery {
            id: delivery.id.clone(),
            run: delivery.run_id.clone(),
            sequence,
            attempt: delivery.attempt_count,
            parent,
            wait,
            episode,
        });
    }
    pub fn committed(&mut self, event: &TaskEventPayload, sequence: i64, parent: Correlation) {
        if self.retired || !pioneer_observability::telemetry_enabled() {
            return;
        }
        let delivery_event = matches!(
            event,
            TaskEventPayload::DeliveryQueued { .. }
                | TaskEventPayload::DeliveryStarted { .. }
                | TaskEventPayload::DeliveryDelivered { .. }
                | TaskEventPayload::DeliveryFailed { .. }
                | TaskEventPayload::DeliveryCancelled { .. }
        );
        if let Some(run) = event.run_id() {
            if !delivery_event
                && self
                    .admissions
                    .iter()
                    .any(|(_, id, floor, _, _)| id == run && sequence <= *floor)
            {
                return;
            }
        }
        if matches!(
            event,
            TaskEventPayload::RunCompleted { .. }
                | TaskEventPayload::RunFailed { .. }
                | TaskEventPayload::RunBlocked { .. }
                | TaskEventPayload::RunCancelled { .. }
                | TaskEventPayload::TaskCompleted { .. }
                | TaskEventPayload::TaskFailed { .. }
                | TaskEventPayload::TaskCancelled { .. }
        ) {
            self.lifecycle = self.lifecycle.saturating_add(1);
            for (task, run, floor, _, closed) in &mut self.admissions {
                if sequence > *floor
                    && event.task_id() == task.as_str()
                    && event.run_id().is_none_or(|id| id == run.as_str())
                {
                    *closed = true;
                }
            }
        }
        match event {
            TaskEventPayload::TaskResultCandidateCreated { candidate } => {
                self.candidate(candidate, sequence, parent)
            }
            TaskEventPayload::TaskResultReviewEventRecorded { review_event } => {
                if let Some(slot) = self.runs.iter_mut().find(|slot| {
                    slot.run == review_event.run_id
                        && slot.candidate == review_event.candidate_id
                        && slot.turn == review_event.task_run_turn_id
                        && slot.sequence < sequence
                }) {
                    slot.sequence = sequence;
                    if let Some(wait) = &slot.wait {
                        wait.progress();
                    }
                    if slot.review_count < slot.reviews.len() {
                        slot.reviews[slot.review_count] = parent;
                        slot.review_count += 1;
                    } else {
                        slot.reviews_overflow = true;
                        Closing::loss(Loss::LinkCapacity);
                    }
                }
            }
            TaskEventPayload::TaskRunEnteredReview {
                task_id,
                run_id,
                candidate_id,
                ..
            } => {
                if self.review_fenced(task_id, run_id, Some(sequence)) {
                    return;
                }
                if let Some(slot) = self.runs.iter_mut().find(|slot| {
                    slot.run == *run_id
                        && slot.candidate == *candidate_id
                        && slot.sequence < sequence
                        && slot.result_fact.is_none()
                }) {
                    slot.sequence = sequence;
                    if slot.wait.is_none() {
                        let mut wait = Closing::start(Phase::ReviewWait, &slot.parent);
                        wait.wait(WaitKind::UserOrProvider);
                        slot.parent = wait.correlation();
                        slot.wait = Some(wait);
                    }
                }
            }
            TaskEventPayload::TaskResultCandidateAccepted { candidate, .. }
            | TaskEventPayload::TaskResultCandidateRejected { candidate, .. }
            | TaskEventPayload::TaskResultCandidateCancelled { candidate, .. } => {
                if !self
                    .runs
                    .iter()
                    .any(|slot| slot.run == candidate.run_id && slot.candidate == candidate.id)
                {
                    self.candidate(candidate, sequence.saturating_sub(1), parent.clone());
                    Closing::loss(Loss::RecoveredFragment);
                }
                let run_fact = self
                    .terminal_runs
                    .iter()
                    .find(|(id, _, _)| id == &candidate.run_id)
                    .map(|(_, _, fact)| *fact);
                if let Some(slot) = self.runs.iter_mut().find(|slot| {
                    slot.run == candidate.run_id
                        && slot.candidate == candidate.id
                        && slot.turn == candidate.task_run_turn_id
                        && slot.round == candidate.round
                        && slot.sequence < sequence
                }) {
                    slot.sequence = sequence;
                    let fact = match candidate.status {
                        TaskResultCandidateStatus::Accepted => Fact::Accepted,
                        TaskResultCandidateStatus::Rejected => Fact::Rejected,
                        _ => Fact::Cancelled,
                    };
                    finish(&mut slot.wait, &mut slot.parent, fact);
                    slot.accepted = candidate.status == TaskResultCandidateStatus::Accepted;
                    if !slot.accepted {
                        end_result(slot, fact);
                    } else if let Some(fact) = run_fact {
                        end_result(slot, fact);
                    }
                    slot.parent = parent;
                }
            }
            TaskEventPayload::RunCompleted { run_id, .. }
            | TaskEventPayload::RunFailed { run_id, .. }
            | TaskEventPayload::RunCancelled { run_id, .. }
            | TaskEventPayload::RunBlocked { run_id, .. } => {
                let run_fact = match event {
                    TaskEventPayload::RunCompleted { .. } => Fact::ExecutionSucceeded,
                    TaskEventPayload::RunCancelled { .. } => Fact::Cancelled,
                    TaskEventPayload::RunBlocked { .. } => Fact::Blocked,
                    _ => Fact::ExecutionFailed,
                };
                self.remember_run_terminal(run_id, sequence, run_fact);
                if !self.runs.iter().any(|slot| slot.run == *run_id) {
                    let task_id = event.task_id();
                    if ids_fit(&[task_id, run_id]) {
                        if self.runs.len() == SLOTS {
                            self.runs.pop_front();
                            Closing::loss(Loss::OwnerCapacity);
                        }
                        self.runs.push_back(Run {
                            task: task_id.to_owned(),
                            run: run_id.clone(),
                            candidate: String::new(),
                            turn: String::new(),
                            round: 0,
                            generation: None,
                            accepted: false,
                            sequence: sequence.saturating_sub(1),
                            parent: parent.clone(),
                            wait: None,
                            result: None,
                            result_fact: None,
                            result_observed: false,
                            reviews: std::array::from_fn(|_| Correlation::default()),
                            review_count: 0,
                            reviews_overflow: false,
                        });
                    }
                }
                for slot in self
                    .runs
                    .iter_mut()
                    .filter(|slot| slot.run == *run_id && slot.sequence < sequence)
                {
                    let fact = run_fact;
                    finish(&mut slot.wait, &mut slot.parent, fact);
                    if !matches!(fact, Fact::ExecutionSucceeded) || slot.accepted {
                        end_result(slot, fact);
                    }
                    slot.parent = parent.clone();
                }
            }
            TaskEventPayload::TaskRevisionRequested {
                run_id,
                previous_candidate_id,
                ..
            } => {
                if let Some(slot) = self.runs.iter_mut().find(|slot| {
                    slot.run == *run_id
                        && slot.candidate == *previous_candidate_id
                        && slot.sequence < sequence
                }) {
                    slot.sequence = sequence;
                    finish(&mut slot.wait, &mut slot.parent, Fact::Superseded);
                    end_result(slot, Fact::Superseded);
                }
            }
            TaskEventPayload::DeliveryQueued { delivery } => {
                self.delivery(delivery, sequence, Fact::Scheduled, None)
            }
            TaskEventPayload::DeliveryStarted { delivery, .. } => {
                self.delivery(delivery, sequence, Fact::Returned, Some(parent))
            }
            TaskEventPayload::DeliveryDelivered { delivery, .. } => {
                self.delivery(delivery, sequence, Fact::Delivered, Some(parent))
            }
            TaskEventPayload::DeliveryFailed { delivery, .. } => self.delivery(
                delivery,
                sequence,
                if delivery.status == TaskDeliveryStatus::Pending {
                    Fact::Retrying
                } else {
                    if delivery.attempt_count >= delivery.max_attempts {
                        Fact::Exhausted
                    } else {
                        Fact::DeliveryFailed
                    }
                },
                Some(parent),
            ),
            TaskEventPayload::DeliveryCancelled { delivery, .. } => {
                self.delivery(delivery, sequence, Fact::Cancelled, Some(parent))
            }
            TaskEventPayload::TaskCancelled { task_id, .. }
            | TaskEventPayload::TaskCompleted { task_id, .. }
            | TaskEventPayload::TaskFailed { task_id, .. } => {
                self.remember_task_terminal(task_id, sequence);
                let fact = if matches!(event, TaskEventPayload::TaskCancelled { .. }) {
                    Fact::Cancelled
                } else {
                    Fact::Unconfirmed // task terminal alone cannot confirm an exact result source
                };
                for slot in self.runs.iter_mut().filter(|slot| {
                    slot.task == *task_id
                        && slot.sequence < sequence
                        && !self
                            .admissions
                            .iter()
                            .any(|(_, id, floor, _, _)| id == &slot.run && sequence <= *floor)
                }) {
                    finish(&mut slot.wait, &mut slot.parent, fact);
                    if matches!(fact, Fact::Cancelled) {
                        end_result(slot, fact);
                    }
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_observability::live::{WaitKind, WorkState};

    async fn fixture() -> (
        crate::TaskRuntime,
        TaskResultCandidate,
        pioneer_protocol::TaskRun,
    ) {
        let runtime = crate::tests::runtime_with_review_config().await;
        let (_, run, candidate) = crate::tests::create_waiting_review_agent_task(
            &runtime,
            2,
            0,
            TaskResultCandidateStatus::PendingReview,
        )
        .await;
        let candidate = runtime
            .service()
            .store()
            .get_task_result_candidate(&candidate)
            .await
            .unwrap()
            .unwrap();
        let run = runtime
            .service()
            .get_task(pioneer_protocol::TaskGetParams {
                task_id: candidate.task_id.clone(),
            })
            .await
            .unwrap()
            .runs
            .into_iter()
            .find(|row| row.id == run)
            .unwrap();
        (runtime, candidate, run)
    }
    fn source(trace: &str) -> Correlation {
        let mut value = serde_json::json!({"_pioneer_correlation": {"v":1,
            "trace":trace, "span":"2222222222222222","flags":1}});
        pioneer_observability::scenario::accept_params("thread/open", &mut value).0
    }
    fn active() -> usize {
        pioneer_observability::live::snapshot()
            .unwrap()
            .active
            .iter()
            .filter(|row| {
                row.context.trace_id == Some([0x66; 16])
                    && matches!(row.state, WorkState::Waiting(WaitKind::UserOrProvider))
            })
            .count()
    }
    #[tokio::test]
    async fn terminal_publication_fences_late_candidates_and_stale_recovery() {
        pioneer_observability::set_telemetry_enabled(true);
        let (_runtime, candidate, run) = fixture().await;
        let parent = source("66666666666666666666666666666666");
        parent.run(|| {
            let mut observer = ClosingObserver::default();
            observer.committed(
                &TaskEventPayload::RunCancelled {
                    task_id: candidate.task_id.clone(),
                    run_id: run.id.clone(),
                    reason: None,
                    cancelled_at: 20,
                },
                20,
                parent.clone(),
            );
            observer.committed(
                &TaskEventPayload::TaskResultCandidateCreated {
                    candidate: candidate.clone(),
                },
                10,
                parent.clone(),
            );
            observer.committed(
                &TaskEventPayload::TaskRunEnteredReview {
                    task_id: candidate.task_id.clone(),
                    run_id: run.id.clone(),
                    candidate_id: candidate.id.clone(),
                    entered_at: 11,
                },
                11,
                parent.clone(),
            );
            observer.recover_review(&candidate, &run);
            assert_eq!(active(), 0);
            // Another run of the same task is not fenced by a run terminal.
            let mut next_run = run.clone();
            next_run.id.push_str("-next");
            let mut next = candidate.clone();
            next.run_id = next_run.id.clone();
            next.id.push_str("-next");
            observer.recover_review(&next, &next_run);
            assert_eq!(active(), 1);
            observer.committed(
                &TaskEventPayload::TaskCancelled {
                    task_id: candidate.task_id.clone(),
                    reason: None,
                    completed_at: 30,
                },
                30,
                parent.clone(),
            );
            assert_eq!(active(), 0);
            next_run.id.push_str("-late");
            next.run_id = next_run.id.clone();
            next.id.push_str("-late");
            observer.recover_review(&next, &next_run);
            assert_eq!(active(), 0);
        });
    }
    #[tokio::test]
    async fn exact_source_terminal_meaning_is_independent_of_handoff_time() {
        pioneer_observability::set_telemetry_enabled(true);
        let (_runtime, c0, run) = fixture().await;
        let parent = source("67676767676767676767676767676767");
        parent.run(|| {
            for early in [true, false] {
                let mut observer = ClosingObserver::default();
                let mut c1 = c0.clone();
                c1.id.push_str("-next");
                c1.task_run_turn_id.push_str("-next");
                c1.round += 1;
                observer.committed(
                    &TaskEventPayload::TaskResultCandidateCreated {
                        candidate: c0.clone(),
                    },
                    10,
                    parent.clone(),
                );
                let mut first = Some(Closing::start(Phase::ChildResult, &parent));
                if early {
                    observer.handoff_result(
                        &run.id,
                        &c0.task_run_turn_id,
                        first.take().unwrap(),
                        None,
                    );
                }
                observer.committed(
                    &TaskEventPayload::TaskRevisionRequested {
                        task_id: c0.task_id.clone(),
                        run_id: run.id.clone(),
                        previous_candidate_id: c0.id.clone(),
                        requested_by_review_event_id: "review".to_owned(),
                        task_run_turn_id: c1.task_run_turn_id.clone(),
                        thread_id: "thread".to_owned(),
                        turn_id: "turn".to_owned(),
                        round: c1.round,
                        feedback: String::new(),
                        requested_at: 12,
                    },
                    12,
                    parent.clone(),
                );
                observer.committed(
                    &TaskEventPayload::TaskResultCandidateCreated {
                        candidate: c1.clone(),
                    },
                    13,
                    parent.clone(),
                );
                c1.status = TaskResultCandidateStatus::Accepted;
                observer.committed(
                    &TaskEventPayload::TaskResultCandidateAccepted {
                        candidate: c1.clone(),
                        review_event_id: "accepted".to_owned(),
                    },
                    14,
                    parent.clone(),
                );
                let mut second = Some(Closing::start(Phase::ChildResult, &parent));
                if early {
                    observer.handoff_result(
                        &run.id,
                        &c1.task_run_turn_id,
                        second.take().unwrap(),
                        None,
                    );
                }
                observer.committed(
                    &TaskEventPayload::RunCompleted {
                        task_id: c0.task_id.clone(),
                        run_id: run.id.clone(),
                        result: None,
                        completed_at: 20,
                    },
                    20,
                    parent.clone(),
                );
                if let Some(first) = first {
                    observer.handoff_result(&run.id, &c0.task_run_turn_id, first, None);
                }
                if let Some(second) = second {
                    observer.handoff_result(&run.id, &c1.task_run_turn_id, second, None);
                }
                let old = observer
                    .runs
                    .iter()
                    .find(|row| row.candidate == c0.id)
                    .unwrap();
                assert!(
                    matches!(old.result_fact, Some(Fact::Superseded))
                        && old.result_observed
                        && old.result.is_none()
                );
                let current = observer
                    .runs
                    .iter()
                    .find(|row| row.candidate == c1.id)
                    .unwrap();
                assert!(
                    matches!(current.result_fact, Some(Fact::ExecutionSucceeded))
                        && current.result_observed
                        && current.result.is_none()
                );
                assert!(
                    !pioneer_observability::live::snapshot()
                        .unwrap()
                        .active
                        .iter()
                        .any(|row| row.context.trace_id == Some([0x67; 16]))
                );
            }
        });
    }
    #[tokio::test]
    async fn accepted_lifecycle_survives_history_eviction_and_same_run_reopen() {
        pioneer_observability::set_telemetry_enabled(true);
        let (_runtime, candidate, run) = fixture().await;
        let parent = source("68686868686868686868686868686868");
        parent.run(|| {
            let mut observer = ClosingObserver::default();
            // Model committed terminal receipts, with no retained active waits.
            for index in 0..=SLOTS {
                observer.committed(
                    &TaskEventPayload::RunCancelled {
                        task_id: format!("past-task-{index}"),
                        run_id: format!("past-run-{index}"),
                        reason: None,
                        cancelled_at: 2,
                    },
                    2,
                    parent.clone(),
                );
            }
            let mut old = candidate.clone();
            old.task_id = "past-task-0".into();
            old.run_id = "past-run-0".into();
            observer.committed(
                &TaskEventPayload::TaskResultCandidateCreated {
                    candidate: old.clone(),
                },
                1,
                parent.clone(),
            );
            assert!(!observer.runs.iter().any(|row| row.candidate == old.id));
            // Oversized historical identity also cannot disable a fresh receipt.
            observer.committed(
                &TaskEventPayload::RunCancelled {
                    task_id: "unknown".into(),
                    run_id: "x".repeat(ID_BYTES + 1),
                    reason: None,
                    cancelled_at: 2,
                },
                2,
                parent.clone(),
            );
            let ticket = observer.ticket();
            observer.admit(ticket, &run.task_id, &run.id, 10, false);
            let first_generation = observer.generation(&run.id);
            observer.committed(
                &TaskEventPayload::TaskResultCandidateCreated {
                    candidate: candidate.clone(),
                },
                11,
                parent.clone(),
            );
            observer.committed(
                &TaskEventPayload::TaskRunEnteredReview {
                    task_id: run.task_id.clone(),
                    run_id: run.id.clone(),
                    candidate_id: candidate.id.clone(),
                    entered_at: 12,
                },
                12,
                parent.clone(),
            );
            assert!(
                pioneer_observability::live::snapshot()
                    .unwrap()
                    .active
                    .iter()
                    .any(|row| row.context.trace_id == Some([0x68; 16])
                        && matches!(row.state, WorkState::Waiting(WaitKind::UserOrProvider)))
            );
            observer.committed(
                &TaskEventPayload::RunBlocked {
                    task_id: run.task_id.clone(),
                    run_id: run.id.clone(),
                    error: None,
                    blocked_at: 20,
                },
                20,
                parent.clone(),
            );
            let ticket = observer.ticket();
            observer.admit(ticket, &run.task_id, &run.id, 30, true);
            assert_ne!(observer.generation(&run.id), first_generation);
            observer.recover_review(&candidate, &run); // pre-block DTO, no sequence proof
            observer.committed(
                &TaskEventPayload::TaskResultCandidateCreated {
                    candidate: candidate.clone(),
                },
                11,
                parent.clone(),
            );
            observer.committed(
                &TaskEventPayload::RunBlocked {
                    task_id: run.task_id.clone(),
                    run_id: run.id.clone(),
                    error: None,
                    blocked_at: 20,
                },
                20,
                parent.clone(),
            );
            assert!(
                !observer
                    .runs
                    .iter()
                    .any(|row| row.candidate == candidate.id)
            );
            let mut resumed = candidate.clone();
            resumed.id.push_str("-resumed");
            observer.committed(
                &TaskEventPayload::TaskResultCandidateCreated {
                    candidate: resumed.clone(),
                },
                31,
                parent.clone(),
            );
            observer.committed(
                &TaskEventPayload::TaskRunEnteredReview {
                    task_id: run.task_id.clone(),
                    run_id: run.id.clone(),
                    candidate_id: resumed.id.clone(),
                    entered_at: 32,
                },
                32,
                parent.clone(),
            );
            observer.handoff_result(
                &run.id,
                &resumed.task_run_turn_id,
                Closing::start(Phase::ChildResult, &parent),
                first_generation,
            );
            assert!(
                observer
                    .runs
                    .iter()
                    .find(|row| row.candidate == resumed.id)
                    .unwrap()
                    .result
                    .is_none()
            );
            let generation = observer.generation(&run.id);
            observer.handoff_result(
                &run.id,
                &resumed.task_run_turn_id,
                Closing::start(Phase::ChildResult, &parent),
                generation,
            );
            resumed.status = TaskResultCandidateStatus::Accepted;
            observer.committed(
                &TaskEventPayload::TaskResultCandidateAccepted {
                    candidate: resumed.clone(),
                    review_event_id: "review".into(),
                },
                33,
                parent.clone(),
            );
            observer.committed(
                &TaskEventPayload::RunCompleted {
                    task_id: run.task_id.clone(),
                    run_id: run.id.clone(),
                    result: None,
                    completed_at: 34,
                },
                34,
                parent.clone(),
            );
            let slot = observer
                .runs
                .iter()
                .find(|row| row.candidate == resumed.id)
                .unwrap();
            assert!(
                matches!(slot.result_fact, Some(Fact::ExecutionSucceeded))
                    && slot.result_observed
                    && slot.result.is_none()
            );
            assert!(
                !pioneer_observability::live::snapshot()
                    .unwrap()
                    .active
                    .iter()
                    .any(|row| row.context.trace_id == Some([0x68; 16]))
            );
        });
    }
}
