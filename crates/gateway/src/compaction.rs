//! Bounded service execution for a previously admitted working-context operation.
//! The native/CLI request preparers own selection and target materialization.
mod admission;
mod background_history;
mod checkpoint;
mod compatible;
mod coordination;
pub(crate) use coordination::{ContextCompactionCoordinator, ContextWorkPriority};
mod coverage;
#[cfg(test)]
pub(crate) use coverage::{CheckpointGraphResolver, observe_preparation_work};
mod delivered;
pub(crate) mod frozen;
mod history;
#[cfg(test)]
pub(crate) use history::pause_selected_turn_load;
mod native;
mod origins;
mod result_budget;
mod service;
#[cfg(test)]
pub(crate) mod test_support;
mod tool_outcomes;
pub(crate) use admission::{PreparedOperation, admit_operation};
pub(crate) use background_history::{HistoryCheckDeadline, prepare_completed_history_owned};
#[cfg(test)]
pub(crate) use background_history::{
    observe_completed_history_preflight, prepare_completed_history,
};
pub(crate) use history::provider_observation;
#[cfg(test)]
pub(crate) use history::{event_message, load_line_history};
#[cfg(test)]
pub(crate) fn legacy_event_message(
    event: pioneer_crud::CanonicalTurnEventPayload,
) -> anyhow::Result<Option<pioneer_provider::ChatMessage>> {
    history::legacy_event_message(event)
}
#[cfg(test)]
pub(crate) use native::observe_prepared_transfers;
pub(crate) use native::{GatewayNativeContextController, native_owner};
pub(crate) use tool_outcomes::{retained_shell_outcome, retained_tool_policy};

use anyhow::{Result, ensure};
use async_trait::async_trait;
use pioneer_compaction::runner::{
    AttemptPurpose, FailureDiagnostic, FailureKind, RunnerAction, RunnerPhase, RunnerState,
    SourceCursor,
};
use pioneer_compaction::summary::{
    ReferenceMaterial, Summarizer, SummaryInput, SummaryPart, SummaryRequest, validate_summary,
};
use pioneer_compaction::{Checkpoint, OperationSnapshot, SourceRef};
use pioneer_crud::{
    CrudStore,
    compaction::{CHECKPOINT_SOURCE_LIMIT, CanonicalFragment, CommitOutcome, SOURCE_PAGE_ROWS},
};
use pioneer_protocol::{
    AgentDurableEvent, ItemCompletedNotification, ItemStartedNotification, SystemEventLevel,
    TurnItemType,
};
use pioneer_runtime_events::ExecutionEventHub;
use std::{
    collections::BTreeMap,
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
    pub lifecycle_store: CrudStore,
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
                &serde_json::json!({"operationId":operation,"attempts":state.attempts,"retries":state.retries,"corrections":state.corrections,"diagnostic":state.diagnostic}),
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
                &self.lifecycle_store,
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
        self.hub.publish_heartbeat(
            self.workspace.clone(),
            self.thread.clone(),
            self.turn.clone(),
            format!("compaction:{operation}"),
            TurnItemType::SystemEvent,
        );
    }
    async fn terminal(&self, operation: &str, state: &RunnerState) -> Result<()> {
        self.processor
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("compaction lifecycle owner stopped"))?
            .publish_compaction_lifecycle(
                &self.lifecycle_store,
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
    source_text_projection_version: u32,
    completed: Vec<SourceRef>,
    final_portion: bool,
}

const RUNNER_FRAGMENT_CHARACTERS: u64 = 16_384;
const RUNNER_INDEX_STRIDE: u64 = 1_024;

fn historical_source_model_payload(
    source: &SourceRef,
    payload: String,
    projection_version: u32,
) -> Result<String> {
    if projection_version == 0 {
        return Ok(payload);
    }
    ensure!(
        projection_version == pioneer_protocol::HISTORICAL_COMMAND_LLM_PROJECTION_VERSION,
        "unsupported historical source text projection"
    );
    if source.scope.starts_with("item:") {
        let Ok(item) = serde_json::from_str::<pioneer_protocol::TurnItem>(&payload) else {
            return Ok(payload);
        };
        return match item.historical_command_llm_projection() {
            Some(projection) => Ok(serde_json::to_string(&projection)?),
            None => Ok(payload),
        };
    }
    if source.scope.starts_with("event:") {
        let Ok(event) = serde_json::from_str::<pioneer_crud::CanonicalTurnEventPayload>(&payload)
        else {
            return Ok(payload);
        };
        return Ok(history::historical_command_event_json(&event)?.unwrap_or(payload));
    }
    Ok(payload)
}

fn source_text_projection_for_cursor(state: &RunnerState) -> u32 {
    if state.source_text_projection_version == 0 && state.cursor.character == 0 {
        pioneer_protocol::HISTORICAL_COMMAND_LLM_PROJECTION_VERSION
    } else {
        state.source_text_projection_version
    }
}

struct IndexedPayload {
    text: String,
    characters: u64,
    checkpoints: Vec<(u64, usize)>,
}

impl IndexedPayload {
    fn new(text: String) -> Self {
        let mut checkpoints = vec![(0, 0)];
        let mut characters = 0_u64;
        for (byte, _) in text.char_indices() {
            if characters > 0 && characters.is_multiple_of(RUNNER_INDEX_STRIDE) {
                checkpoints.push((characters, byte));
            }
            characters += 1;
        }
        Self {
            text,
            characters,
            checkpoints,
        }
    }

    fn fragment(&self, source: &SourceRef, character_offset: u64) -> Result<CanonicalFragment> {
        ensure!(
            character_offset <= self.characters,
            "source offset is past end"
        );
        let checkpoint = self
            .checkpoints
            .partition_point(|(character, _)| *character <= character_offset)
            .saturating_sub(1);
        let (mut character, base_byte) = self.checkpoints[checkpoint];
        let mut start_byte = base_byte;
        for (relative, value) in self.text[base_byte..].char_indices() {
            if character == character_offset {
                start_byte = base_byte + relative;
                break;
            }
            character += 1;
            start_byte = base_byte + relative + value.len_utf8();
        }
        let mut end_byte = start_byte;
        let mut taken = 0_u64;
        for (relative, value) in self.text[start_byte..].char_indices() {
            if taken == RUNNER_FRAGMENT_CHARACTERS {
                break;
            }
            end_byte = start_byte + relative + value.len_utf8();
            taken += 1;
        }
        let end = character_offset.saturating_add(taken);
        Ok(CanonicalFragment {
            reference: source.clone(),
            text: self.text[start_byte..end_byte].to_owned(),
            next_character: (end < self.characters).then_some(end),
        })
    }
}

struct ActivePayload {
    thread: String,
    source: SourceRef,
    projection_version: u32,
    payload: Arc<IndexedPayload>,
}

#[derive(Clone)]
struct ReferenceExcerpt {
    thread: String,
    source: SourceRef,
    text: String,
}

pub(crate) struct CompactionRunner {
    store: CrudStore,
    workspace: String,
    snapshot: OperationSnapshot,
    summarizer: Arc<dyn Summarizer>,
    target: Arc<dyn CompactionTarget>,
    observer: Arc<dyn CompactionObserver>,
    clock: Arc<dyn CompactionClock>,
    // Operation-local and revision-keyed. Only the active full object is kept;
    // reference-only material is retained as bounded excerpts.
    active_payload: tokio::sync::Mutex<Option<ActivePayload>>,
    reference_excerpts: tokio::sync::OnceCell<Vec<ReferenceExcerpt>>,
    #[cfg(test)]
    active_payload_loads: std::sync::atomic::AtomicUsize,
}
impl CompactionRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: CrudStore,
        workspace: String,
        _thread: String,
        snapshot: OperationSnapshot,
        summarizer: Arc<dyn Summarizer>,
        target: Arc<dyn CompactionTarget>,
        observer: Arc<dyn CompactionObserver>,
        clock: Arc<dyn CompactionClock>,
    ) -> Self {
        Self {
            store,
            workspace,
            snapshot,
            summarizer,
            target,
            observer,
            clock,
            active_payload: tokio::sync::Mutex::new(None),
            reference_excerpts: tokio::sync::OnceCell::new(),
            #[cfg(test)]
            active_payload_loads: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    async fn active_payload_fragment(
        &self,
        thread: &str,
        source: &SourceRef,
        character_offset: u64,
    ) -> Result<CanonicalFragment> {
        self.active_payload_fragment_with_projection(
            thread,
            source,
            character_offset,
            pioneer_protocol::HISTORICAL_COMMAND_LLM_PROJECTION_VERSION,
        )
        .await
    }

    async fn active_payload_fragment_with_projection(
        &self,
        thread: &str,
        source: &SourceRef,
        character_offset: u64,
        projection_version: u32,
    ) -> Result<CanonicalFragment> {
        let cached = {
            let mut cache = self.active_payload.lock().await;
            if let Some(active) = cache.as_ref()
                && active.thread == thread
                && active.source == *source
                && active.projection_version == projection_version
            {
                Some(active.payload.clone())
            } else {
                // Release the previous decompressed object before the next DB
                // read can materialize another one.
                *cache = None;
                None
            }
        };
        let payload = match cached {
            Some(payload) => {
                ensure!(
                    self.store
                        .compaction_references_current(
                            &self.workspace,
                            thread,
                            std::slice::from_ref(source),
                        )
                        .await?,
                    "source unavailable or stale"
                );
                payload
            }
            None => {
                let text = self
                    .store
                    .compaction_reference_payload(&self.workspace, thread, source)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("source unavailable or stale"))?;
                #[cfg(test)]
                self.active_payload_loads
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Projection and index construction happen after the reader has
                // been released. The cache is revision- and representation-keyed.
                let text = historical_source_model_payload(source, text, projection_version)?;
                let payload = Arc::new(IndexedPayload::new(text));
                *self.active_payload.lock().await = Some(ActivePayload {
                    thread: thread.to_owned(),
                    source: source.clone(),
                    projection_version,
                    payload: payload.clone(),
                });
                payload
            }
        };
        payload.fragment(source, character_offset)
    }

    #[cfg(test)]
    async fn reference_excerpts(&self) -> Result<&Vec<ReferenceExcerpt>> {
        self.reference_excerpts_with_projection(
            pioneer_protocol::HISTORICAL_COMMAND_LLM_PROJECTION_VERSION,
        )
        .await
    }

    async fn reference_excerpts_with_projection(
        &self,
        projection_version: u32,
    ) -> Result<&Vec<ReferenceExcerpt>> {
        self.reference_excerpts
            .get_or_try_init(|| async {
                let entries = self
                    .store
                    .compaction_manifest_page(&self.snapshot.id, true, 0, 0)
                    .await?;
                let mut excerpts = Vec::with_capacity(entries.len());
                for entry in entries {
                    self.store
                        .compaction_prepare_references(
                            &self.workspace,
                            &entry.thread_id,
                            std::slice::from_ref(&entry.source),
                        )
                        .await?;
                    let payload = self
                        .store
                        .compaction_reference_payload(
                            &self.workspace,
                            &entry.thread_id,
                            &entry.source,
                        )
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("reference source unavailable or stale"))?;
                    let payload = historical_source_model_payload(
                        &entry.source,
                        payload,
                        projection_version,
                    )?;
                    let mut characters = payload.chars();
                    let excerpt = characters
                        .by_ref()
                        .take(usize::try_from(RUNNER_FRAGMENT_CHARACTERS)?)
                        .collect::<String>();
                    let text = if characters.next().is_some() {
                        format!(
                            "{excerpt}\n[Reference-only excerpt; remaining source not included.]"
                        )
                    } else {
                        excerpt
                    };
                    excerpts.push(ReferenceExcerpt {
                        thread: entry.thread_id,
                        source: entry.source,
                        text,
                    });
                    // `payload` is dropped here before the next reference is read.
                }
                Ok(excerpts)
            })
            .await
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
        let mut publication_validation_retries = 0_u32;
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
                    if !self
                        .store
                        .compaction_manifest_sources_current(operation)
                        .await?
                    {
                        state = self
                            .persist(
                                &state,
                                Self::diagnose(
                                    state.terminate(FailureKind::Permanent)?,
                                    FailureDiagnostic::new(
                                        "source_validation",
                                        "source_revision_changed",
                                        "History projection changed after admission",
                                    ),
                                ),
                                None,
                            )
                            .await?;
                        continue;
                    }
                    let portion = match self.portion(&state, purpose).await {
                        Ok(portion) => portion,
                        Err(_) => {
                            state = self
                                .persist(&state, Self::diagnose(state.terminate(FailureKind::Permanent)?,
                                    FailureDiagnostic::new("portion_preparation", "source_preparation_failed", "Could not prepare the next portion from admitted source revisions")), None)
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
                        diagnostic: None,
                    });
                    state = self.persist(&state, claimed, None).await?;
                    let RunnerPhase::Attempt { deadline_ms, .. } = state.phase else {
                        unreachable!()
                    };
                    let mut result = tokio::select! { biased;
                        _=self.clock.sleep_until(deadline_ms)=>None,
                        result=self.summarizer.summarize(portion.request.clone())=>Some(result),
                    };
                    // No validation/publication/retry can get ahead of the
                    // previous service process and transport being released.
                    if let Err(failure) = self.summarizer.cleanup().await {
                        // Preserve the original failure when cleanup also fails.
                        if !matches!(result, Some(Err(_))) {
                            result = Some(Err(failure));
                        }
                    }
                    match result {
                        None => {
                            state = self
                                .persist(
                                    &state,
                                    Self::diagnose(
                                        self.attempt_failed(&state, FailureKind::Transient, None)?,
                                        FailureDiagnostic::new(
                                            "summarization",
                                            "attempt_deadline",
                                            "Summary attempt exceeded its deadline",
                                        ),
                                    ),
                                    None,
                                )
                                .await?;
                        }
                        Some(Err(failure)) => {
                            state = self
                                .persist(
                                    &state,
                                    Self::diagnose(
                                        self.attempt_failed(
                                            &state,
                                            failure.kind,
                                            failure.retry_after_ms,
                                        )?,
                                        failure.diagnostic.unwrap_or_else(|| {
                                            FailureDiagnostic::new(
                                                "summarization",
                                                failure.code,
                                                "Summarizer rejected the request",
                                            )
                                        }),
                                    ),
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
                            let summary = match validate_summary(
                                &completion,
                                portion.request.output_cap,
                            ) {
                                Ok(text) => text,
                                Err(_) => {
                                    state = self
                                            .persist(
                                                &state,
                                                Self::diagnose(state.attempt_failed(
                                                    FailureKind::InvalidCompletion,
                                                    self.clock.now_ms(),
                                                    None,
                                                )?, FailureDiagnostic::new("summary_validation", "invalid_completion", "Summary completion did not satisfy the required format or output budget")),
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
                            let mut next = state.candidate(
                                state.attempts,
                                checkpoint.id.clone(),
                                portion.cursor,
                                portion.final_portion,
                                self.clock.now_ms(),
                            )?;
                            next.source_text_projection_version =
                                portion.source_text_projection_version;
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
                        CommitOutcome::RetryValidation => {
                            // The proof raced a dependency mutation. Keep the
                            // Commit phase and its durable summary; this delay
                            // occurs after both reader and writer resources are
                            // released and consumes no provider retry budget.
                            let shift = publication_validation_retries.min(5);
                            let delay_ms = 10_u64.saturating_mul(1_u64 << shift);
                            publication_validation_retries =
                                publication_validation_retries.saturating_add(1);
                            let wake = self
                                .clock
                                .now_ms()
                                .saturating_add(delay_ms)
                                .min(self.snapshot.admission.deadline_ms);
                            self.clock.sleep_until(wake).await;
                        }
                        CommitOutcome::Stale => {
                            state = self
                                .persist(&state, Self::diagnose(state.terminate(FailureKind::Permanent)?,
                                    FailureDiagnostic::new("checkpoint_commit", "checkpoint_stale", "Checkpoint was not applied: source revision, accepted import, coverage or context head no longer matches admission")), None)
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

    fn diagnose(mut state: RunnerState, diagnostic: FailureDiagnostic) -> RunnerState {
        // Post-summary validation failures belong to the operation, not to the
        // successful provider attempt. Failed calls retain their own diagnostic.
        if let Some(observation) = &mut state.observation
            && observation.failure.is_some()
        {
            observation.diagnostic = Some(diagnostic.clone());
        }
        state.diagnostic = Some(diagnostic);
        state
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
                coverage_domain: self.snapshot.plan.coverage_domain,
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
                source_text_projection_version: state.source_text_projection_version,
                completed: vec![],
                final_portion: true,
            });
        }
        // Build bounded reference-only excerpts before materializing the active
        // full payload. They are reused across portions and retries.
        let source_text_projection_version = source_text_projection_for_cursor(state);
        let finish_legacy_source_before_upgrade =
            state.source_text_projection_version == 0 && state.cursor.character > 0;
        // Reference-only excerpts carry no cursor or coverage, so they can use
        // the current projection even while a legacy active source drains.
        let reference_excerpts = self
            .reference_excerpts_with_projection(
                pioneer_protocol::HISTORICAL_COMMAND_LLM_PROJECTION_VERSION,
            )
            .await?;
        let mut reference_scopes = BTreeMap::<&str, Vec<SourceRef>>::new();
        for excerpt in reference_excerpts {
            reference_scopes
                .entry(&excerpt.thread)
                .or_default()
                .push(excerpt.source.clone());
        }
        for (thread, references) in reference_scopes {
            ensure!(
                self.store
                    .compaction_references_current(&self.workspace, thread, &references)
                    .await?,
                "reference source unavailable or stale"
            );
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
        self.store
            .compaction_prepare_references(
                &self.workspace,
                &first.thread_id,
                std::slice::from_ref(&first.source),
            )
            .await?;
        // Reference-only bodies are optional context. They never acquire coverage.
        // Reserve a source fragment before adding them, so they cannot starve work.
        let initial = self
            .active_payload_fragment_with_projection(
                &first.thread_id,
                &first.source,
                state.cursor.character,
                source_text_projection_version,
            )
            .await?;
        let reserve_part = SummaryPart {
            sources: vec![first.source.clone()],
            unit: first.unit,
            part: state.cursor.character,
            last_part: false,
            text: initial.text,
        };
        for excerpt in reference_excerpts {
            let mut proposed = request.clone();
            proposed.input.reference_only.push(ReferenceMaterial {
                source: excerpt.source.clone(),
                text: excerpt.text.clone(),
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
            self.store
                .compaction_prepare_references(
                    &self.workspace,
                    &entry.thread_id,
                    std::slice::from_ref(&entry.source),
                )
                .await?;
            let fragment = self
                .active_payload_fragment_with_projection(
                    &entry.thread_id,
                    &entry.source,
                    cursor.character,
                    source_text_projection_version,
                )
                .await?;
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
                        source_text_projection_version,
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
                    source_text_projection_version,
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
                if finish_legacy_source_before_upgrade {
                    return Ok(Portion {
                        request,
                        cursor,
                        source_text_projection_version,
                        completed,
                        final_portion: next.is_none(),
                    });
                }
                if finishes_unit {
                    boundary = Some(Portion {
                        request: request.clone(),
                        cursor,
                        source_text_projection_version,
                        completed: completed.clone(),
                        final_portion: next.is_none(),
                    });
                }
                if next.is_none() {
                    return Ok(Portion {
                        request,
                        cursor,
                        source_text_projection_version,
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
            source_text_projection_version,
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
