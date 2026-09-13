//! Owned background preparation of completed CLI history. Queue rows contain
//! locators/settings only; the existing canonical history remains authoritative.
use super::*;
use crate::compaction::{CompactionClock, SystemCompactionClock};
use pioneer_compaction::{CompactionSettings, ModelSelection, Transport};
use serde::{Deserialize, Serialize};
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
        for row in store.compaction_pending_history_checks().await? {
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
                let result = processor
                    .run_completed_history_check(&row, task_cancel, task_suspending.clone())
                    .await;
                if !task_suspending.load(Ordering::Acquire) {
                    let outcome = if result.is_ok() {
                        "completed"
                    } else {
                        "failed"
                    };
                    if processor
                        .crud_store
                        .compaction_finish_history_check(&row.turn_id, outcome)
                        .await
                        .is_err()
                    {
                        warn!("failed to persist CLI history check outcome");
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
    ) -> anyhow::Result<()> {
        if !self
            .crud_store
            .compaction_history_check_is_current(&row.turn_id)
            .await?
        {
            self.crud_store
                .compaction_finish_history_check(&row.turn_id, "cancelled")
                .await?;
            return Ok(());
        }
        let descriptor = if let Some(descriptor) = &row.descriptor {
            descriptor.clone()
        } else {
            let (settings, cli_override) = {
                let legacy = self.compaction_settings()?;
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
            let current = ModelSelection {
                transport: match row.runtime_kind.as_str() {
                    "codex" => Transport::Codex,
                    "claude" => Transport::Claude,
                    _ => anyhow::bail!("unknown CLI history transport"),
                },
                instance: row.runtime_id.clone(),
                model: row
                    .model
                    .clone()
                    .filter(|model| !model.trim().is_empty())
                    .ok_or_else(|| anyhow::anyhow!("completed CLI model is unavailable"))?,
                effort: row.reasoning_effort.clone(),
            };
            let captured = CapturedCheck {
                target_output_cap: None,
                fixed_input_tokens: 0,
                current,
                settings,
                cli_override,
                deadline_ms: SystemCompactionClock::default()
                    .now_ms()
                    .saturating_add(pioneer_compaction::OPERATION_MILLIS),
            };
            let descriptor = serde_json::to_string(&captured)?;
            let Some(descriptor) = self
                .crud_store
                .compaction_capture_history_check(&row.turn_id, &descriptor)
                .await?
            else {
                return Ok(());
            };
            descriptor
        };
        let captured: CapturedCheck = serde_json::from_str(&descriptor)?;
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
                cancel,
                Some(captured.deadline_ms),
                captured.target_output_cap,
                captured.fixed_input_tokens,
                suspending,
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
        result.map(|_| ())
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
