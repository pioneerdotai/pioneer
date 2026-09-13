//! Bounded service execution for a previously admitted working-context operation.
//! The native/CLI request preparers own selection and target materialization.
mod admission;
mod background_history;
mod checkpoint;
mod compatible;
mod coordination;
pub(crate) use coordination::{ContextCompactionCoordinator, ContextWorkPriority};
mod coverage;
mod delivered;
pub(crate) mod frozen;
mod history;
mod native;
mod origins;
mod result_budget;
mod service;
#[cfg(test)]
pub(crate) mod test_support;
mod tool_outcomes;
pub(crate) use admission::{PreparedOperation, admit_operation};
#[cfg(test)]
pub(crate) use background_history::prepare_completed_history;
pub(crate) use background_history::prepare_completed_history_owned;
#[cfg(test)]
pub(crate) use history::load_line_history;
pub(crate) use history::provider_observation;
pub(crate) use native::{GatewayNativeContextController, native_owner};
pub(crate) use tool_outcomes::{retained_shell_outcome, retained_tool_policy};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use pioneer_compaction::runner::{
    AttemptPurpose, FailureKind, RunnerAction, RunnerPhase, RunnerState, SourceCursor,
};
use pioneer_compaction::summary::{
    ReferenceMaterial, Summarizer, SummaryInput, SummaryPart, SummaryRequest, validate_summary,
};
use pioneer_compaction::{Checkpoint, OperationSnapshot, SourceRef};
use pioneer_crud::{
    CrudStore,
    compaction::{CHECKPOINT_SOURCE_LIMIT, CommitOutcome, SOURCE_PAGE_ROWS},
};
use pioneer_protocol::{
    AgentDurableEvent, AgentProgressEvent, ItemCompletedNotification, ItemHeartbeatSource,
    ItemStartedNotification, SystemEventLevel, TurnItemType,
};
use pioneer_runtime_events::ExecutionEventHub;
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub(crate) trait CompactionClock: Send + Sync {
    fn now_ms(&self) -> u64;
    async fn sleep_until(&self, deadline_ms: u64);
}
pub(crate) struct SystemCompactionClock {
    epoch: u64,
    start: tokio::time::Instant,
}
impl Default for SystemCompactionClock {
    fn default() -> Self {
        Self {
            epoch: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u64::MAX as u128) as u64,
            start: tokio::time::Instant::now(),
        }
    }
}
#[async_trait]
impl CompactionClock for SystemCompactionClock {
    fn now_ms(&self) -> u64 {
        self.epoch
            .saturating_add(self.start.elapsed().as_millis().min(u64::MAX as u128) as u64)
    }
    async fn sleep_until(&self, deadline_ms: u64) {
        tokio::time::sleep(Duration::from_millis(
            deadline_ms.saturating_sub(self.now_ms()),
        ))
        .await;
    }
}

/// Must rematerialize the complete target request with the proposed summary.
/// A valid summary alone cannot approve the working-context switch.
#[async_trait]
pub(crate) trait CompactionTarget: Send + Sync {
    async fn fits(&self, summary: &str) -> Result<bool>;
}
pub(crate) struct NativeRequestTarget(
    pub pioneer_agent::compaction::request::NativeRequestProjection,
);
#[async_trait]
impl CompactionTarget for NativeRequestTarget {
    async fn fits(&self, summary: &str) -> Result<bool> {
        Ok(self.0.evaluate(summary)?.fits)
    }
}

#[async_trait]
pub(crate) trait CompactionObserver: Send + Sync {
    async fn started(&self, operation: &str, state: &RunnerState) -> Result<()>;
    fn heartbeat(&self, operation: &str);
    async fn terminal(&self, operation: &str, state: &RunnerState) -> Result<()>;
}

/// Reuses the existing context-compaction timeline item for all portions/retries.
pub(crate) struct HubCompactionObserver {
    pub hub: Arc<ExecutionEventHub>,
    pub processor: std::sync::Weak<crate::message::MessageProcessor>,
    pub workspace: String,
    pub thread: String,
    pub turn: String,
}
impl HubCompactionObserver {
    fn item(
        &self,
        operation: &str,
        state: &RunnerState,
        terminal: bool,
    ) -> pioneer_protocol::TurnItem {
        let completed = matches!(state.phase, RunnerPhase::Applied { .. });
        let (status, message, level) = if !terminal {
            (
                "started",
                "Context compaction started",
                SystemEventLevel::Info,
            )
        } else if completed {
            (
                "completed",
                "Context compaction completed",
                SystemEventLevel::Info,
            )
        } else if matches!(
            state.phase,
            RunnerPhase::Failed {
                kind: FailureKind::Cancelled
            }
        ) {
            (
                "cancelled",
                "Context compaction cancelled",
                SystemEventLevel::Info,
            )
        } else {
            (
                "failed",
                "Context compaction failed",
                SystemEventLevel::Error,
            )
        };
        crate::cli_runtime::projector::context_compaction_item(
            &format!("compaction:{operation}"),
            level,
            message,
            status,
            Some(
                &serde_json::json!({"operationId":operation,"attempts":state.attempts,"retries":state.retries,"corrections":state.corrections}),
            ),
        )
    }
}
#[async_trait]
impl CompactionObserver for HubCompactionObserver {
    async fn started(&self, operation: &str, state: &RunnerState) -> Result<()> {
        self.processor
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("compaction lifecycle owner stopped"))?
            .publish_compaction_lifecycle(
                operation,
                state.generation,
                AgentDurableEvent::ItemStarted {
                    notification: ItemStartedNotification {
                        workspace_id: self.workspace.clone(),
                        thread_id: self.thread.clone(),
                        turn_id: self.turn.clone(),
                        item: self.item(operation, state, false),
                    },
                },
            )
            .await?;
        Ok(())
    }
    fn heartbeat(&self, operation: &str) {
        self.hub
            .publish_progress(AgentProgressEvent::ItemHeartbeat {
                workspace_id: self.workspace.clone(),
                thread_id: self.thread.clone(),
                turn_id: self.turn.clone(),
                item_id: format!("compaction:{operation}"),
                item_type: TurnItemType::SystemEvent,
                source: ItemHeartbeatSource::OwnerLease,
            });
    }
    async fn terminal(&self, operation: &str, state: &RunnerState) -> Result<()> {
        self.processor
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("compaction lifecycle owner stopped"))?
            .publish_compaction_lifecycle(
                operation,
                state.generation,
                AgentDurableEvent::ItemCompleted {
                    notification: ItemCompletedNotification {
                        workspace_id: self.workspace.clone(),
                        thread_id: self.thread.clone(),
                        turn_id: self.turn.clone(),
                        item: self.item(operation, state, true),
                    },
                },
            )
            .await?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CompactionExit {
    Applied(String),
    Failed(FailureKind),
    /// A deadline/Stop raced work. The owner must inspect durable operation/head
    /// state before reporting a commit outcome or attempting any continuation.
    Reconcile(FailureKind),
}
struct Portion {
    request: SummaryRequest,
    cursor: SourceCursor,
    completed: Vec<SourceRef>,
    final_portion: bool,
}

pub(crate) struct CompactionRunner {
    store: CrudStore,
    workspace: String,
    thread: String,
    snapshot: OperationSnapshot,
    summarizer: Arc<dyn Summarizer>,
    target: Arc<dyn CompactionTarget>,
    observer: Arc<dyn CompactionObserver>,
    clock: Arc<dyn CompactionClock>,
}
impl CompactionRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: CrudStore,
        workspace: String,
        thread: String,
        snapshot: OperationSnapshot,
        summarizer: Arc<dyn Summarizer>,
        target: Arc<dyn CompactionTarget>,
        observer: Arc<dyn CompactionObserver>,
        clock: Arc<dyn CompactionClock>,
    ) -> Self {
        Self {
            store: store.with_maintenance_access(),
            workspace,
            thread,
            snapshot,
            summarizer,
            target,
            observer,
            clock,
        }
    }
    /// No provider, observer or source transformation runs while DB capacity is held.
    /// Dropping drive cancels queued DB work and the owned summarizer transport.
    pub async fn run(&self, cancel: CancellationToken) -> Result<CompactionExit> {
        let result = {
            let work = self.drive();
            tokio::pin!(work);
            let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! { biased;
                    _ = cancel.cancelled() => break Ok(CompactionExit::Reconcile(FailureKind::Cancelled)),
                    _ = self.clock.sleep_until(self.snapshot.admission.deadline_ms) => break Ok(CompactionExit::Reconcile(FailureKind::Deadline)),
                    result = &mut work => break result,
                    _ = heartbeat.tick() => self.observer.heartbeat(&self.snapshot.id),
                }
            }
        };
        // Drop drive (and its attempt future) before taking the adapter's
        // cleanup ownership. Joining is outside every database scope.
        self.summarizer.cleanup().await?;
        result
    }
    /// Read durable truth after an interrupted service future. This is an owned
    /// control-plane reconciliation, never a new service attempt or deadline.
    /// Completed operations can therefore be recovered after their 15 minutes.
    pub async fn reconcile(&self, reason: FailureKind) -> Result<CompactionExit> {
        ensure!(
            matches!(reason, FailureKind::Cancelled | FailureKind::Deadline),
            "invalid reconciliation reason"
        );
        let reason = if self
            .store
            .compaction_execution_cancelled(&self.snapshot.id)
            .await?
        {
            FailureKind::Cancelled
        } else {
            reason
        };
        let (status, outcome) = if reason == FailureKind::Cancelled {
            ("cancelled", "cancelled")
        } else {
            ("failed", "deadline")
        };
        self.store
            .compaction_finish(&self.snapshot.id, status, outcome)
            .await?;
        // The caller may have dropped run itself on its enclosing deadline.
        self.summarizer.cleanup().await?;
        let durable = self
            .store
            .compaction_operation(&self.snapshot.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("operation missing during reconciliation"))?;
        let state = self
            .store
            .compaction_reconcile_runner_state(&self.snapshot.id)
            .await?;
        if durable.status == "completed" {
            let state = state.ok_or_else(|| anyhow::anyhow!("completed state missing"))?;
            let RunnerPhase::Applied { checkpoint } = &state.phase else {
                anyhow::bail!("completed operation has no applied checkpoint")
            };
            if self
                .observer
                .terminal(&self.snapshot.id, &state)
                .await
                .is_err()
            {
                tracing::warn!("compaction terminal observer unavailable");
            }
            return Ok(CompactionExit::Applied(checkpoint.clone()));
        }
        let kind = if durable.status == "cancelled" {
            FailureKind::Cancelled
        } else if durable.outcome.as_deref() == Some("deadline") {
            FailureKind::Deadline
        } else if let Some(RunnerState {
            phase: RunnerPhase::Failed { kind },
            ..
        }) = &state
        {
            kind.clone()
        } else {
            FailureKind::Permanent
        };
        if let Some(state) = state {
            if self
                .observer
                .terminal(&self.snapshot.id, &state)
                .await
                .is_err()
            {
                tracing::warn!("compaction terminal observer unavailable");
            }
        }
        Ok(CompactionExit::Failed(kind))
    }

    async fn drive(&self) -> Result<CompactionExit> {
        let operation = &self.snapshot.id;
        let plan = self
            .store
            .compaction_runner_plan(operation)
            .await?
            .ok_or_else(|| anyhow::anyhow!("runner plan missing"))?;
        ensure!(
            plan.ready && plan.budget == self.summarizer.model_budget(),
            "runner model budget changed after admission"
        );
        let mut state = self
            .store
            .compaction_runner_state(operation)
            .await?
            .ok_or_else(|| anyhow::anyhow!("runner state missing"))?;
        if !matches!(state.action(self.clock.now_ms()), RunnerAction::Terminal) {
            if self.observer.started(operation, &state).await.is_err() {
                tracing::warn!("compaction observer unavailable");
            }
        }
        loop {
            let durable = self
                .store
                .compaction_operation(operation)
                .await?
                .ok_or_else(|| anyhow::anyhow!("compaction operation missing"))?;
            if durable.status == "completed" {
                let applied = self
                    .store
                    .compaction_runner_state(operation)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("completed runner state missing"))?;
                let RunnerPhase::Applied { checkpoint } = &applied.phase else {
                    anyhow::bail!("completed operation has no applied runner checkpoint")
                };
                if self.observer.terminal(operation, &applied).await.is_err() {
                    tracing::warn!("compaction terminal observer unavailable");
                }
                return Ok(CompactionExit::Applied(checkpoint.clone()));
            }
            if durable.status != "running" {
                return self
                    .reconcile(if durable.status == "cancelled" {
                        FailureKind::Cancelled
                    } else {
                        FailureKind::Deadline
                    })
                    .await;
            }
            if self.store.compaction_execution_cancelled(operation).await? {
                state = self
                    .persist(&state, state.terminate(FailureKind::Cancelled)?, None)
                    .await?;
            }
            if self
                .store
                .compaction_projection_version(&self.workspace, &self.thread)
                .await?
                != self.snapshot.projection_version
            {
                state = self
                    .persist(&state, state.terminate(FailureKind::Permanent)?, None)
                    .await?;
            }
            match state.action(self.clock.now_ms()) {
                RunnerAction::Expired => {
                    return Ok(CompactionExit::Reconcile(FailureKind::Deadline));
                }
                RunnerAction::Terminal => {
                    if self.observer.terminal(operation, &state).await.is_err() {
                        tracing::warn!("compaction terminal observer unavailable");
                    }
                    return Ok(match &state.phase {
                        RunnerPhase::Applied { checkpoint } => {
                            CompactionExit::Applied(checkpoint.clone())
                        }
                        RunnerPhase::Failed { kind } => CompactionExit::Failed(kind.clone()),
                        _ => unreachable!(),
                    });
                }
                RunnerAction::WaitUntil(deadline) => self.clock.sleep_until(deadline).await,
                RunnerAction::RecoverInterruptedAttempt => {
                    let next = self.attempt_failed(&state, FailureKind::Transient, None)?;
                    state = self.persist(&state, next, None).await?;
                }
                RunnerAction::Prepare(purpose) => {
                    let portion = match self.portion(&state, purpose).await {
                        Ok(portion) => portion,
                        Err(_) => {
                            state = self
                                .persist(&state, state.terminate(FailureKind::Permanent)?, None)
                                .await?;
                            continue;
                        }
                    };
                    let mut claimed = state.claim(self.clock.now_ms())?;
                    claimed.observation = Some(pioneer_compaction::runner::AttemptObservation {
                        number: claimed.attempts,
                        started_ms: self.clock.now_ms(),
                        finished_ms: None,
                        estimated_input_tokens: self.summarizer.input_tokens(&portion.request)?,
                        output_cap: portion.request.output_cap,
                        input_tokens: None,
                        output_tokens: None,
                        completion: None,
                        failure: None,
                    });
                    state = self.persist(&state, claimed, None).await?;
                    let RunnerPhase::Attempt { deadline_ms, .. } = state.phase else {
                        unreachable!()
                    };
                    let result = tokio::select! { biased;
                        _=self.clock.sleep_until(deadline_ms)=>None,
                        result=self.summarizer.summarize(portion.request.clone())=>Some(result),
                    };
                    // No validation/publication/retry can get ahead of the
                    // previous service process and transport being released.
                    self.summarizer.cleanup().await?;
                    match result {
                        None => {
                            state = self
                                .persist(
                                    &state,
                                    self.attempt_failed(&state, FailureKind::Transient, None)?,
                                    None,
                                )
                                .await?;
                        }
                        Some(Err(failure)) => {
                            state = self
                                .persist(
                                    &state,
                                    self.attempt_failed(
                                        &state,
                                        failure.kind,
                                        failure.retry_after_ms,
                                    )?,
                                    None,
                                )
                                .await?;
                        }
                        Some(Ok(completion)) => {
                            if let Some(observation) = &mut state.observation {
                                observation.finished_ms = Some(self.clock.now_ms());
                                observation.completion = Some(completion.kind);
                                observation.input_tokens = completion.input_tokens;
                                observation.output_tokens = completion.output_tokens;
                            }
                            let summary =
                                match validate_summary(&completion, portion.request.output_cap) {
                                    Ok(text) => text,
                                    Err(_) => {
                                        state = self
                                            .persist(
                                                &state,
                                                state.attempt_failed(
                                                    FailureKind::InvalidCompletion,
                                                    self.clock.now_ms(),
                                                    None,
                                                )?,
                                                None,
                                            )
                                            .await?;
                                        continue;
                                    }
                                };
                            let checkpoint = Checkpoint {
                                id: format!("{}:summary:{}", operation, state.attempts),
                                operation_id: operation.clone(),
                                owner: self.snapshot.owner.clone(),
                                previous: state.previous_checkpoint.clone(),
                                coverage: portion.completed,
                                summary,
                                selection: self.snapshot.admission.selection.clone(),
                                projection_version: self.snapshot.projection_version,
                                format_version: 1,
                            };
                            let next = state.candidate(
                                state.attempts,
                                checkpoint.id.clone(),
                                portion.cursor,
                                portion.final_portion,
                                self.clock.now_ms(),
                            )?;
                            state = self.persist(&state, next, Some(&checkpoint)).await?;
                        }
                    }
                }
                RunnerAction::ValidateCandidate {
                    checkpoint,
                    final_portion,
                } => {
                    let candidate = self
                        .store
                        .compaction_checkpoint(&checkpoint)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("candidate missing"))?;
                    let fits = final_portion && self.target.fits(&candidate.summary).await?;
                    state = self
                        .persist(&state, state.candidate_checked(fits)?, None)
                        .await?;
                }
                RunnerAction::Commit(checkpoint) => {
                    match self
                        .store
                        .compaction_apply_runner(
                            operation,
                            &state,
                            self.snapshot.expected_checkpoint.as_deref(),
                        )
                        .await?
                    {
                        CommitOutcome::Applied | CommitOutcome::AlreadyApplied => {
                            let applied = state.applied(&checkpoint)?;
                            // Observer failures cannot roll back or regenerate a committed summary.
                            if self.observer.terminal(operation, &applied).await.is_err() {
                                tracing::warn!("compaction terminal observer unavailable");
                            }
                            return Ok(CompactionExit::Applied(checkpoint));
                        }
                        CommitOutcome::Cancelled => {
                            return Ok(CompactionExit::Reconcile(FailureKind::Cancelled));
                        }
                        CommitOutcome::Stale => {
                            state = self
                                .persist(&state, state.terminate(FailureKind::Permanent)?, None)
                                .await?;
                        }
                    }
                }
            }
        }
    }
    fn attempt_failed(
        &self,
        state: &RunnerState,
        kind: FailureKind,
        retry_after_ms: Option<u64>,
    ) -> Result<RunnerState> {
        // Stable per-operation jitter follows the project's recovery pattern.
        // The resulting not_before time is persisted, so restart cannot redraw
        // a shorter wait or reset Retry-After and the operation deadline.
        let delay = if kind == FailureKind::Transient && state.retries < 2 {
            let base = [2_000_u64, 8_000][state.retries as usize];
            let hash = self
                .snapshot
                .id
                .bytes()
                .fold(state.retries as u64, |acc, byte| {
                    acc.wrapping_mul(131).wrapping_add(byte as u64)
                });
            Some((base + hash % (base / 8 + 1)).max(retry_after_ms.unwrap_or(0)))
        } else {
            retry_after_ms
        };
        state.attempt_failed(kind, self.clock.now_ms(), delay)
    }

    async fn persist(
        &self,
        previous: &RunnerState,
        next: RunnerState,
        candidate: Option<&Checkpoint>,
    ) -> Result<RunnerState> {
        ensure!(
            self.store
                .compaction_runner_transition(
                    &self.snapshot.id,
                    previous.generation,
                    &next,
                    candidate
                )
                .await?,
            "runner transition superseded or cancelled"
        );
        Ok(next)
    }
    fn request(&self, state: &RunnerState, previous_summary: String) -> SummaryRequest {
        SummaryRequest {
            selection: self.snapshot.admission.selection.clone(),
            output_cap: state.target_tokens,
            input: SummaryInput {
                mode: self.snapshot.plan.mode,
                previous_summary,
                compact_units: vec![],
                reference_only: vec![],
                target_tokens: state.target_tokens,
            },
        }
    }
    fn fits(&self, request: &SummaryRequest) -> Result<bool> {
        Ok(self.summarizer.model_budget().fits(
            self.summarizer.input_tokens(request)?,
            request.output_cap,
            false,
        ))
    }
    async fn portion(&self, state: &RunnerState, purpose: AttemptPurpose) -> Result<Portion> {
        let previous = if let Some(id) = &state.previous_checkpoint {
            self.store
                .compaction_checkpoint(id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("previous summary missing"))?
                .summary
        } else {
            String::new()
        };
        let mut request = self.request(state, previous);
        if purpose == AttemptPurpose::Correction {
            ensure!(
                self.fits(&request)?,
                "correction input exceeds model capacity"
            );
            return Ok(Portion {
                request,
                cursor: state.cursor,
                completed: vec![],
                final_portion: true,
            });
        }
        let first = self
            .store
            .compaction_manifest_page(
                &self.snapshot.id,
                false,
                state.cursor.unit,
                state.cursor.source,
            )
            .await?;
        let first = first
            .first()
            .ok_or_else(|| anyhow::anyhow!("no source progress available"))?;
        if first.source.scope.starts_with("checkpoint:")
            && Some(first.source.id.as_str()) == self.snapshot.expected_checkpoint.as_deref()
            && state.previous_checkpoint == self.snapshot.expected_checkpoint
        {
            request.input.previous_summary.clear();
        }
        // Reference-only bodies are optional context. They never acquire coverage.
        // Reserve a source fragment before adding them, so they cannot starve work.
        let initial = self
            .store
            .compaction_reference_fragment(
                &self.workspace,
                &first.thread_id,
                &first.source,
                state.cursor.character,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("source unavailable or stale"))?;
        let reserve_part = SummaryPart {
            sources: vec![first.source.clone()],
            unit: first.unit,
            part: state.cursor.character,
            last_part: false,
            text: initial.text,
        };
        for entry in self
            .store
            .compaction_manifest_page(&self.snapshot.id, true, 0, 0)
            .await?
        {
            let fragment = self
                .store
                .compaction_reference_fragment(&self.workspace, &entry.thread_id, &entry.source, 0)
                .await?
                .ok_or_else(|| anyhow::anyhow!("reference source unavailable or stale"))?;
            let text = if fragment.next_character.is_some() {
                format!(
                    "{}\n[Reference-only excerpt; remaining source not included.]",
                    fragment.text
                )
            } else {
                fragment.text
            };
            let mut proposed = request.clone();
            proposed.input.reference_only.push(ReferenceMaterial {
                source: entry.source,
                text,
            });
            proposed.input.compact_units.push(reserve_part.clone());
            if self.fits(&proposed)? {
                proposed.input.compact_units.clear();
                request = proposed;
            }
        }
        let mut cursor = state.cursor;
        let mut completed = Vec::new();
        let mut boundary: Option<Portion> = None;
        for _ in 0..SOURCE_PAGE_ROWS {
            let page = self
                .store
                .compaction_manifest_page(&self.snapshot.id, false, cursor.unit, cursor.source)
                .await?;
            let Some(entry) = page.first() else { break };
            let fragment = self
                .store
                .compaction_reference_fragment(
                    &self.workspace,
                    &entry.thread_id,
                    &entry.source,
                    cursor.character,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("source unavailable or stale"))?;
            let next = page.get(1);
            let source_index = if entry.unit == cursor.unit {
                cursor.source
            } else {
                0
            };
            let finishes_unit = fragment.next_character.is_none()
                && next.is_none_or(|next| next.unit != entry.unit);
            let part = SummaryPart {
                sources: vec![entry.source.clone()],
                unit: entry.unit,
                part: cursor.character,
                last_part: finishes_unit,
                text: fragment.text,
            };
            let mut proposed = request.clone();
            proposed.input.compact_units.push(part.clone());
            if !self.fits(&proposed)? {
                if let Some(boundary) = boundary {
                    return Ok(boundary);
                }
                if !request.input.compact_units.is_empty() {
                    return Ok(Portion {
                        request,
                        cursor,
                        completed,
                        final_portion: false,
                    });
                }
                let mut low = 0;
                let mut high = part.text.len();
                let mut best = 0;
                while low <= high {
                    let mid = low + (high - low) / 2;
                    let mut end = mid;
                    while !part.text.is_char_boundary(end) {
                        end -= 1;
                    }
                    proposed.input.compact_units[0].text = part.text[..end].to_owned();
                    proposed.input.compact_units[0].last_part = false;
                    if self.fits(&proposed)? {
                        best = end;
                        low = mid + 1;
                    } else if mid == 0 {
                        break;
                    } else {
                        high = mid - 1;
                    }
                }
                ensure!(best > 0, "summary framing leaves no source capacity");
                proposed.input.compact_units[0].text = part.text[..best].to_owned();
                let character = cursor
                    .character
                    .checked_add(proposed.input.compact_units[0].text.chars().count() as u64)
                    .ok_or_else(|| anyhow::anyhow!("source cursor overflow"))?;
                return Ok(Portion {
                    request: proposed,
                    cursor: SourceCursor {
                        unit: entry.unit,
                        source: source_index,
                        character,
                    },
                    completed,
                    final_portion: false,
                });
            }
            request = proposed;
            if let Some(character) = fragment.next_character {
                cursor = SourceCursor {
                    unit: entry.unit,
                    source: source_index,
                    character,
                };
            } else {
                completed.push(entry.source.clone());
                ensure!(
                    completed.len() <= CHECKPOINT_SOURCE_LIMIT,
                    "portion coverage exceeds quantum"
                );
                cursor = match next {
                    Some(next) if next.unit == entry.unit => SourceCursor {
                        unit: entry.unit,
                        source: source_index
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("source index overflow"))?,
                        character: 0,
                    },
                    Some(next) => SourceCursor {
                        unit: next.unit,
                        source: 0,
                        character: 0,
                    },
                    None => SourceCursor {
                        unit: entry
                            .unit
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("unit cursor overflow"))?,
                        source: 0,
                        character: 0,
                    },
                };
                if finishes_unit {
                    boundary = Some(Portion {
                        request: request.clone(),
                        cursor,
                        completed: completed.clone(),
                        final_portion: next.is_none(),
                    });
                }
                if next.is_none() {
                    return Ok(Portion {
                        request,
                        cursor,
                        completed,
                        final_portion: true,
                    });
                }
            }
        }
        if let Some(boundary) = boundary {
            return Ok(boundary);
        }
        ensure!(
            !request.input.compact_units.is_empty(),
            "portion made no progress"
        );
        Ok(Portion {
            request,
            cursor,
            completed,
            final_portion: false,
        })
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) fn load_test_catalog() {
    static READY: std::sync::Once = std::sync::Once::new();
    READY.call_once(|| {
        let directory = tempfile::tempdir().unwrap();
        let saved = serde_json::json!({
            "version": 1,
            "updated_at": "2026-09-12T00:00:00Z",
            "catalog": {
                "models": serde_json::from_str::<serde_json::Value>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../provider/tests/fixtures/catalog/models.json"))).unwrap(),
                "provenance": serde_json::from_str::<serde_json::Value>(include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../provider/tests/fixtures/catalog/provenance.json"))).unwrap(),
            }
        });
        std::fs::write(directory.path().join("catalog.json"), serde_json::to_vec(&saved).unwrap()).unwrap();
        pioneer_provider::catalog::runtime::restore_cached_catalog(directory.path()).unwrap();
    });
}
