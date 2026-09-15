//! Owned background preparation of completed CLI history. Queue rows contain
//! locators/settings only; the existing canonical history remains authoritative.
use super::*;
use crate::compaction::{CompactionClock, SystemCompactionClock};
use pioneer_compaction::{CompactionSettings, ModelSelection, Transport};
use pioneer_crud::compaction::{HistoryCheckDiagnostic, HistoryCheckOutcome};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::atomic::AtomicBool;
use tokio_util::sync::CancellationToken;

#[derive(Serialize, Deserialize)]
struct CapturedCheck {
    current: ModelSelection,
    settings: CompactionSettings,
    cli_override: Option<ModelSelection>,
    deadline_ms: u64,
    #[serde(default)]
    target_output_cap: Option<u32>,
    #[serde(default)]
    fixed_input_tokens: u64,
}
pub(super) struct OwnedHistoryCheck {
    turn: String,
    cancel: CancellationToken,
    suspending: Arc<AtomicBool>,
    pub(super) handle: Option<JoinHandle<()>>,
}
impl Drop for OwnedHistoryCheck {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
impl MessageProcessor {
    /// Gradually register pre-compaction histories without delaying ordinary
    /// CLI turns. One thread is advanced by one bounded database quantum per
    /// resilience interval. A failed candidate is skipped until the cursor
    /// wraps, so later histories continue to make progress.
    pub(crate) async fn run_legacy_history_preparation_worker(processor: Weak<Self>) {
        let mut after_thread = String::new();
        let mut active = None::<(String, String)>;
        loop {
            let mut idle = false;
            let Some(this) = processor.upgrade() else {
                break;
            };
            let this = this.for_background_reconciliation();
            let store = this.crud_store.with_maintenance_access();
            if active.is_none() {
                let mut candidate = store
                    .compaction_history_preparation_candidate_after(&after_thread)
                    .await;
                if matches!(candidate, Ok(None)) && !after_thread.is_empty() {
                    after_thread.clear();
                    candidate = store
                        .compaction_history_preparation_candidate_after(&after_thread)
                        .await;
                }
                match candidate {
                    Ok(candidate) => {
                        idle = candidate.is_none();
                        active = candidate;
                    }
                    Err(_) => warn!("failed to discover legacy history preparation work"),
                }
            }
            if let Some((workspace, thread)) = active.as_ref() {
                match store
                    .compaction_prepare_history_quantum(workspace, thread)
                    .await
                {
                    Ok(true) => {
                        after_thread.clone_from(thread);
                        active = None;
                    }
                    Ok(false) => {}
                    Err(_) => {
                        warn!("failed to advance legacy history preparation");
                        after_thread.clone_from(thread);
                        active = None;
                    }
                }
            }
            // Once every legacy history is ready, only probe occasionally.
            // Threads created after the migration are marked ready by trigger.
            let delay = if idle {
                Duration::from_secs(60)
            } else {
                Duration::from_secs(RESILIENCE_WORKER_POLL_INTERVAL_SECONDS)
            };
            sleep(delay).await;
        }
    }

    pub(crate) async fn enqueue_native_completed_history(
        &self,
        context: &pioneer_agent::compaction::controller::NativeContext,
        mut request: pioneer_provider::ChatRequest,
    ) -> anyhow::Result<()> {
        let settings = self.compaction_settings_for_workspace(&context.workspace_id)?;
        let clock = SystemCompactionClock::default();
        let deadline = clock
            .now_ms()
            .saturating_add(pioneer_compaction::OPERATION_MILLIS);
        let prepare = async {
            // Persist only the cost of fixed instructions/tools/media, not their
            // contents or any conversational messages. The additive background
            // estimate conservatively includes both request envelopes. Foreground
            // materialization always recalculates its complete actual request.
            request
                .messages
                .retain(|message| message.role == pioneer_provider::Role::System);
            let limits = pioneer_provider::catalog::model_catalog()?
                .limits(context.provider.name(), &request.model);
            let budget = pioneer_compaction::ModelBudget::new(
                Some(limits.context_window),
                limits.max_input,
                limits.max_output,
            );
            let materialized = context.provider.prepare_input_budget(request).await?;
            let fixed = pioneer_agent::compaction::request::NativeRequestProjection::full(
                materialized.request,
                materialized.media,
                budget,
                false,
            )?;
            let current = ModelSelection {
                transport: Transport::Api,
                instance: context.provider_instance.clone(),
                model: fixed.request.model.clone(),
                effort: match fixed.request.reasoning {
                    Some(pioneer_provider::ReasoningConfig::Effort(effort)) => {
                        Some(effort.as_str().to_owned())
                    }
                    Some(pioneer_provider::ReasoningConfig::Disabled) => Some("none".to_owned()),
                    None => None,
                },
            };
            let captured = CapturedCheck {
                current,
                settings,
                cli_override: None,
                target_output_cap: Some(u32::try_from(fixed.output_reserve)?),
                fixed_input_tokens: fixed.estimated_input_tokens,
                deadline_ms: deadline,
            };
            let descriptor = serde_json::to_string(&captured)?;
            self.crud_store
                .with_maintenance_access()
                .compaction_enqueue_native_history_check(
                    &context.workspace_id,
                    &context.thread_id,
                    &context.turn_id,
                    &descriptor,
                )
                .await
        };
        tokio::select! { biased;
            _=context.cancellation.cancelled()=>anyhow::bail!("background history registration cancelled"),
            _=clock.sleep_until(deadline)=>anyhow::bail!("background history registration deadline exceeded"),
            result=prepare=>result,
        }
    }
    /// Existing resilience owner polls only durable completion intents, not all
    /// histories. Service work runs separately and never stalls its poll loop.
    pub(crate) async fn poll_completed_history_checks(&self) -> anyhow::Result<()> {
        if !self.agent_manager.has_context_controller().await {
            return Ok(());
        }
        self.reconcile_compaction_lifecycle().await?;
        let store = self.crud_store.with_maintenance_access();
        let finished = {
            let mut jobs = self.completed_history_checks.lock().await;
            let keys: Vec<_> = jobs
                .iter()
                .filter(|(_, job)| job.handle.as_ref().is_none_or(JoinHandle::is_finished))
                .map(|(key, _)| key.clone())
                .collect();
            keys.into_iter()
                .filter_map(|key| jobs.remove(&key))
                .collect::<Vec<_>>()
        };
        for mut job in finished {
            if let Some(handle) = job.handle.take() {
                let _ = handle.await;
            }
        }
        for mut row in store
            .compaction_due_history_checks(SystemCompactionClock::default().now_ms() as i64)
            .await?
        {
            let key = (row.workspace_id.clone(), row.thread_id.clone());
            let mut jobs = self.completed_history_checks.lock().await;
            if jobs.contains_key(&key) {
                continue;
            }
            // Bounded service ownership. Unclaimed durable rows remain pending.
            if jobs.len() >= 16 {
                break;
            }
            let processor =
                Arc::new(self.scoped_with_database_class(SqliteWriteClass::Maintenance));
            let cancel = CancellationToken::new();
            let suspending = Arc::new(AtomicBool::new(false));
            let task_cancel = cancel.clone();
            let task_suspending = suspending.clone();
            let turn = row.turn_id.clone();
            let handle = tokio::spawn(async move {
                let now = SystemCompactionClock::default().now_ms();
                let claimed = tokio::select! { biased;
                    _ = task_cancel.cancelled() => return,
                    result = processor.crud_store.compaction_claim_history_check(&row.turn_id, row.revision, now as i64) => result,
                };
                let claimed = match claimed {
                    Ok(Some(value)) => value,
                    Ok(None) => return,
                    Err(_) => {
                        warn!("failed to claim history check");
                        return;
                    }
                };
                let mut diagnostic = claimed
                    .diagnostic
                    .as_deref()
                    .and_then(|value| serde_json::from_str::<HistoryCheckDiagnostic>(value).ok())
                    .unwrap_or_default();
                diagnostic.legacy_reason_unknown |=
                    row.managed == 0 && row.outcome.as_deref() == Some("failed");
                row.revision = claimed.revision;
                row.attempt_deadline_ms = claimed.attempt_deadline_ms;
                row.descriptor = claimed.descriptor;
                row.config_hash = claimed.config_hash;
                row.failures = claimed.failures;
                row.outcome = claimed.outcome;
                let result = processor
                    .run_completed_history_check(
                        &row,
                        task_cancel.clone(),
                        task_suspending.clone(),
                        &mut diagnostic,
                    )
                    .await;
                if !task_suspending.load(Ordering::Acquire) {
                    let outcome =
                        classify_check_result(result, &mut diagnostic, task_cancel.is_cancelled());
                    if outcome == HistoryCheckOutcome::Retryable && row.failures >= 3 {
                        diagnostic
                            .explanation
                            .push_str("; automatic preparation retry limit reached");
                    }
                    diagnostic.observed_ms = SystemCompactionClock::default().now_ms();
                    if processor
                        .crud_store
                        .compaction_record_history_result(
                            &row.turn_id,
                            row.revision,
                            row.failures,
                            outcome,
                            &diagnostic,
                            diagnostic.observed_ms as i64,
                        )
                        .await
                        .is_err()
                    {
                        warn!("failed to persist history check result");
                    }
                }
            });
            jobs.insert(
                key,
                OwnedHistoryCheck {
                    turn,
                    cancel,
                    suspending,
                    handle: Some(handle),
                },
            );
        }
        Ok(())
    }
    async fn reconcile_compaction_lifecycle(&self) -> anyhow::Result<()> {
        use crate::compaction::CompactionObserver;
        let processor = Arc::new(self.scoped_with_database_class(SqliteWriteClass::Maintenance));
        let store = &processor.crud_store;
        let after = self
            .compaction_recovery_cursor
            .read()
            .map_err(|_| anyhow::anyhow!("recovery cursor unavailable"))?
            .clone();
        let rows = store
            .compaction_lifecycle_recovery(SystemCompactionClock::default().now_ms(), &after)
            .await?;
        *self
            .compaction_recovery_cursor
            .write()
            .map_err(|_| anyhow::anyhow!("recovery cursor unavailable"))? =
            rows.last().map(|row| row.id.clone()).unwrap_or_default();
        for row in rows {
            // Advance even if a row fails. A poison record cannot starve the
            // remainder; the cursor wraps and permits a later bounded retry.
            let result = async {
                let token = CancellationToken::new();
                let Some(_lease) = processor
                    .compaction_coordinator
                    .acquire(
                        &row.workspace_id,
                        &row.thread_id,
                        crate::compaction::ContextWorkPriority::Background,
                        &token,
                    )
                    .await?
                else {
                    return Ok::<_, anyhow::Error>(());
                };
                if row.status == "running" {
                    let (status, outcome) = if row.cancelled {
                        ("cancelled", "cancelled")
                    } else {
                        ("failed", "deadline")
                    };
                    store.compaction_finish(&row.id, status, outcome).await?;
                }
                if let Some(state) = store.compaction_reconcile_runner_state(&row.id).await? {
                    let hub = Arc::new(pioneer_runtime_events::ExecutionEventHub::new());
                    let observer = crate::compaction::HubCompactionObserver {
                        hub: hub.clone(),
                        processor: Arc::downgrade(&processor),
                        workspace: row.workspace_id,
                        thread: row.thread_id,
                        turn: row.turn_id,
                    };
                    let result = observer.terminal(&row.id, &state).await;
                    hub.shutdown_progress().await;
                    result?;
                }
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if result.is_err() {
                warn!("failed to reconcile compaction lifecycle record");
            }
        }
        Ok(())
    }
    async fn run_completed_history_check(
        self: &Arc<Self>,
        row: &pioneer_crud::compaction::CompletedHistoryCheck,
        cancel: CancellationToken,
        suspending: Arc<AtomicBool>,
        diagnostic: &mut HistoryCheckDiagnostic,
    ) -> anyhow::Result<HistoryCheckOutcome> {
        let previous_diagnostic = diagnostic.clone();
        diagnostic.stage = "currency".into();
        let current = tokio::select! { biased;
            _ = cancel.cancelled() => return Ok(HistoryCheckOutcome::Cancelled),
            result = self.crud_store.compaction_history_check_is_current(&row.turn_id) => result?,
        };
        if !current {
            return Ok(HistoryCheckOutcome::Cancelled);
        }
        diagnostic.stage = "configuration".into();
        let legacy = self.compaction_settings()?;
        let (settings, cli_override) = {
            let workspace = self
                .workspace_compaction_settings
                .read()
                .map_err(|_| anyhow::anyhow!("workspace settings unavailable"))?;
            match workspace.get(&row.workspace_id) {
                Some(value) => (
                    value.compaction(&legacy),
                    value.cli_overrides.get(&row.runtime_id).cloned(),
                ),
                None => (legacy, None),
            }
        };
        diagnostic.stage = "descriptor".into();
        let mut captured: CapturedCheck = if let Some(descriptor) = &row.descriptor {
            serde_json::from_str(descriptor)?
        } else {
            let transport = match row.runtime_kind.as_str() {
                "codex" => Transport::Codex,
                "claude" => Transport::Claude,
                _ => {
                    diagnostic.code = "unknown_transport".into();
                    return Ok(HistoryCheckOutcome::Failed);
                }
            };
            let Some(model) = row.model.clone().filter(|model| !model.trim().is_empty()) else {
                diagnostic.code = "missing_turn_model".into();
                return Ok(HistoryCheckOutcome::Failed);
            };
            CapturedCheck {
                current: ModelSelection {
                    transport,
                    instance: row.runtime_id.clone(),
                    model,
                    effort: row.reasoning_effort.clone(),
                },
                settings: settings.clone(),
                cli_override: cli_override.clone(),
                deadline_ms: 0,
                target_output_cap: None,
                fixed_input_tokens: 0,
            }
        };
        diagnostic.stage = "configuration".into();
        // Compare only relevant settings/authorities. No secrets or paths are
        // persisted; the fingerprint is a one-way digest. Waiting never calls a model.
        let waiting_settings = row.outcome.as_deref() == Some("waiting_settings");
        let (hash_settings, hash_override) = if waiting_settings {
            (&settings, &cli_override)
        } else {
            (&captured.settings, &captured.cli_override)
        };
        let candidate = pioneer_compaction::effective_selection(
            &captured.current,
            hash_settings.selection.as_ref(),
            hash_override.as_ref(),
        );
        let registry = self.provider_registry();
        let authority = |selection: &ModelSelection| -> anyhow::Result<String> {
            Ok(match selection.transport {
                Transport::Api => registry
                    .authority_fingerprint_for_workspace(&row.workspace_id, &selection.instance)
                    .map(|v| v.as_str().to_owned())
                    .unwrap_or_else(|_| "unavailable".into()),
                _ => serde_json::to_string(
                    &self
                        .load_cli_runtime_instances()?
                        .into_iter()
                        .find(|instance| instance.id == selection.instance),
                )?,
            })
        };
        let hash = hex::encode(Sha256::digest(serde_json::to_vec(&(
            candidate,
            &captured.current,
            authority(candidate)?,
            authority(&captured.current)?,
        ))?));
        if row.outcome.as_deref() == Some("waiting_settings") {
            if row.config_hash.as_deref() == Some(&hash) {
                *diagnostic = previous_diagnostic;
                return Ok(HistoryCheckOutcome::WaitingSettings);
            }
            captured.settings = settings;
            captured.cli_override = cli_override;
        }
        // A resumed attempt retains its deadline and captured settings. Only a
        // scheduled pre-admission retry/readiness wakeup receives a fresh deadline.
        let now = SystemCompactionClock::default().now_ms();
        let descriptor = serde_json::to_string(&captured)?;
        let deadline = tokio::select! { biased;
            _ = cancel.cancelled() => return Ok(HistoryCheckOutcome::Cancelled),
            result = self.crud_store.compaction_begin_history_attempt(&row.turn_id, row.revision, &descriptor, &hash, now as i64) => result?,
        };
        let Some(deadline) = deadline else {
            return Ok(HistoryCheckOutcome::Cancelled);
        };
        captured.deadline_ms = deadline as u64;
        *diagnostic = HistoryCheckDiagnostic {
            attempt: row.failures.saturating_add(1).max(1) as u64,
            legacy_reason_unknown: diagnostic.legacy_reason_unknown,
            ..HistoryCheckDiagnostic::default()
        };
        let hub = Arc::new(pioneer_runtime_events::ExecutionEventHub::new());
        let mut progress = hub.subscribe_live();
        let observer = Arc::new(crate::compaction::HubCompactionObserver {
            hub: hub.clone(),
            processor: Arc::downgrade(self),
            workspace: row.workspace_id.clone(),
            thread: row.thread_id.clone(),
            turn: row.turn_id.clone(),
        });
        let result = {
            let work = crate::compaction::prepare_completed_history_owned(
                self,
                &row.workspace_id,
                &row.thread_id,
                &row.turn_id,
                &captured.current,
                &captured.settings,
                captured.cli_override.as_ref(),
                observer,
                cancel.clone(),
                Some(captured.deadline_ms),
                captured.target_output_cap,
                captured.fixed_input_tokens,
                suspending,
                diagnostic,
            );
            tokio::pin!(work);
            loop {
                tokio::select! { biased;
                    result = &mut work => break result,
                    event = progress.recv() => if let Ok(event) = event { self.handle_progress_agent_event(event).await; },
                }
            }
        };
        hub.shutdown_progress().await;
        result
    }
    pub(crate) async fn interrupt_completed_history_for_new_input(
        &self,
        workspace: &str,
        thread: &str,
    ) {
        let jobs = self.completed_history_checks.lock().await;
        if let Some(job) = jobs.get(&(workspace.to_owned(), thread.to_owned())) {
            job.cancel.cancel();
        }
    }
    pub(crate) async fn stop_completed_history_check(
        &self,
        workspace: &str,
        thread: &str,
        turn: &str,
    ) {
        let job = {
            let mut jobs = self.completed_history_checks.lock().await;
            let key = (workspace.to_owned(), thread.to_owned());
            if jobs.get(&key).is_some_and(|job| job.turn == turn) {
                jobs.remove(&key)
            } else {
                None
            }
        };
        if let Some(mut job) = job {
            job.cancel.cancel();
            if let Some(handle) = job.handle.take() {
                let _ = handle.await;
            }
        }
    }
    pub(crate) async fn suspend_completed_history_checks(&self) {
        let jobs = std::mem::take(&mut *self.completed_history_checks.lock().await);
        for job in jobs.values() {
            job.suspending.store(true, Ordering::Release);
            job.cancel.cancel();
        }
        for (_, mut job) in jobs {
            if let Some(handle) = job.handle.take() {
                let _ = handle.await;
            }
        }
    }
}

/// Never persist arbitrary provider/SQL errors: fixed codes and explanations
/// identify the failing boundary without retaining payloads or credentials.
fn classify_check_result(
    result: anyhow::Result<HistoryCheckOutcome>,
    diagnostic: &mut HistoryCheckDiagnostic,
    cancelled: bool,
) -> HistoryCheckOutcome {
    if cancelled && !matches!(&result, Ok(HistoryCheckOutcome::Compacted)) {
        diagnostic.code = "cancelled".into();
        diagnostic.explanation = "Check cancelled by Stop, newer input or foreground work".into();
        return HistoryCheckOutcome::Cancelled;
    }
    match result {
        Ok(outcome) => {
            if diagnostic.code.is_empty() {
                diagnostic.code = outcome.as_str().into();
            }
            if diagnostic.explanation.is_empty() {
                diagnostic.explanation = match outcome {
                HistoryCheckOutcome::Fits => "Complete retained history fits the measured budget; no summary generated",
                HistoryCheckOutcome::Compacted => "Summary applied to the context head",
                HistoryCheckOutcome::Preparing => "History registration is incomplete; persisted cursor will resume",
                HistoryCheckOutcome::WaitingCatalog => "Model catalog has not loaded",
                HistoryCheckOutcome::WaitingExecutor => "Context executor is occupied",
                HistoryCheckOutcome::WaitingSettings => "Waiting for a relevant model or provider configuration change",
                HistoryCheckOutcome::Failed => "Compaction operation finished unsuccessfully; its retry budget is not reset",
                HistoryCheckOutcome::Cancelled => "Check is no longer current",
                HistoryCheckOutcome::Retryable => "Temporary preparation failure; bounded retry scheduled",
            }.into();
            }
            outcome
        }
        Err(error) => {
            // A database error is typed; payload text is deliberately discarded.
            let database = error.downcast_ref::<sea_orm::DbErr>().is_some();
            if error
                .downcast_ref::<crate::compaction::HistoryCheckDeadline>()
                .is_some()
                && matches!(
                    diagnostic.stage.as_str(),
                    "history_capture" | "history_preparation"
                )
            {
                diagnostic.code = "history_preparation_incomplete".into();
                diagnostic.explanation =
                    "Preparation time quantum ended; no incomplete history was budgeted".into();
                return HistoryCheckOutcome::Preparing;
            }
            let outcome = if diagnostic.operation.is_some()
                || matches!(diagnostic.stage.as_str(), "admission" | "summarization")
            {
                HistoryCheckOutcome::Failed
            } else if database {
                HistoryCheckOutcome::Retryable
            } else {
                match diagnostic.stage.as_str() {
                    "target_configuration" | "summary_configuration" => {
                        HistoryCheckOutcome::WaitingSettings
                    }
                    "descriptor" | "planning" | "budget" => HistoryCheckOutcome::Failed,
                    _ => HistoryCheckOutcome::Retryable,
                }
            };
            diagnostic.code = if database {
                "database_error"
            } else {
                match diagnostic.stage.as_str() {
                    "descriptor" => "invalid_descriptor",
                    "planning" => "no_fitting_plan",
                    "budget" => "invalid_budget",
                    "target_configuration" | "summary_configuration" => "model_configuration_error",
                    "history_capture" => "history_capture_error",
                    "history_preparation" => "history_preparation_error",
                    "admission" => "operation_admission_error",
                    "summarization" => "operation_execution_error",
                    _ => "check_preparation_error",
                }
            }
            .into();
            diagnostic.explanation = format!(
                "Check stopped during {}. Raw error payload omitted",
                diagnostic.stage
            );
            // Recognized validation errors have fixed public explanations. Only
            // exact known strings are accepted; arbitrary text is never copied.
            for cause in error.chain().take(8) {
                let known = match cause.to_string().as_str() {
                    "compaction source revisions or accepted imports do not match the admitted manifest" => {
                        Some((
                            "invalid_source_manifest",
                            "Source revisions or accepted import evidence do not match the selected history; no provider call was made",
                        ))
                    }
                    "selected CLI service instance is unavailable" => Some((
                        "summary_instance_missing",
                        "Selected CLI summary instance is not configured",
                    )),
                    "selected CLI service instance is disabled" => Some((
                        "summary_instance_disabled",
                        "Selected CLI summary instance is disabled",
                    )),
                    "selected CLI service instance changed kind" => Some((
                        "summary_transport_changed",
                        "Configured CLI kind does not match the selected transport",
                    )),
                    "explicit output limit is not supported by the selected model" => Some((
                        "output_limit_unsupported",
                        "Requested output limit exceeds model capabilities",
                    )),
                    "selected history source disappeared" => Some((
                        "history_source_missing",
                        "A referenced canonical history source is missing",
                    )),
                    "context authority workspace changed" => Some((
                        "history_scope_changed",
                        "History authorization no longer matches the workspace",
                    )),
                    _ => None,
                };
                if let Some((code, explanation)) = known {
                    diagnostic.code = code.into();
                    diagnostic.explanation = explanation.into();
                    break;
                }
            }
            outcome
        }
    }
}

#[cfg(test)]
mod result_tests {
    use super::*;
    #[test]
    fn history_check_failure_policy_never_restarts_a_provider_budget() {
        for stage in ["admission", "summarization"] {
            let mut d = HistoryCheckDiagnostic::new(stage, "", "");
            let result = Err(anyhow::Error::new(sea_orm::DbErr::Custom(
                "private SQL and credentials".into(),
            )));
            assert_eq!(
                classify_check_result(result, &mut d, false),
                HistoryCheckOutcome::Failed
            );
            assert!(!serde_json::to_string(&d).unwrap().contains("private SQL"));
        }
        let mut d = HistoryCheckDiagnostic::new("history_capture", "", "");
        assert_eq!(
            classify_check_result(
                Err(anyhow::Error::new(sea_orm::DbErr::Custom("private".into()))),
                &mut d,
                false
            ),
            HistoryCheckOutcome::Retryable
        );
        assert_eq!(d.code, "database_error");
        let mut d = HistoryCheckDiagnostic::new("summary_configuration", "", "");
        assert_eq!(
            classify_check_result(
                Err(anyhow::anyhow!("selected CLI service instance is disabled")),
                &mut d,
                false
            ),
            HistoryCheckOutcome::WaitingSettings
        );
    }
    #[test]
    fn history_check_wait_cancel_and_fit_remain_distinct() {
        let mut d = HistoryCheckDiagnostic::new("history_capture", "", "");
        assert_eq!(
            classify_check_result(
                Err(crate::compaction::HistoryCheckDeadline.into()),
                &mut d,
                false
            ),
            HistoryCheckOutcome::Preparing
        );
        assert_eq!(
            classify_check_result(Ok(HistoryCheckOutcome::Fits), &mut d, true),
            HistoryCheckOutcome::Cancelled
        );
        let mut d = HistoryCheckDiagnostic::new("budget", "", "");
        d.estimated_input_tokens = Some(1000);
        d.context_tokens = Some(272000);
        assert_eq!(
            classify_check_result(Ok(HistoryCheckOutcome::Fits), &mut d, false),
            HistoryCheckOutcome::Fits
        );
        assert_eq!(d.code, "fits");
        assert_eq!(d.estimated_input_tokens, Some(1000));
        assert!(d.checkpoint.is_none());
    }
}
