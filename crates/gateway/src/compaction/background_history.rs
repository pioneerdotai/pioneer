//! Prepare completed native or completed history for later Pioneer continuations.
//! Every actual native model call independently budgets its complete request.
use super::*;
use pioneer_agent::compaction::{history::NativeHistoryLayout, request::NativeRequestProjection};
use pioneer_compaction::{
    CompactionMode, CompactionSettings, ModelBudget, ModelSelection, SourceRole, Transport,
    effective_selection, plan_compaction,
};
use pioneer_crud::compaction::{HistoryCheckDiagnostic, HistoryCheckOutcome, ManifestEntry};
use pioneer_provider::ChatRequest;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Debug)]
pub(crate) struct HistoryCheckDeadline;
impl std::fmt::Display for HistoryCheckDeadline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("history check preparation deadline")
    }
}
impl std::error::Error for HistoryCheckDeadline {}

/// The completed-turn owner supplies a captured whole selection. Changes to
/// General/instance settings apply only to later jobs. The owner must retain
/// and cancel/join this future at new input, Stop and shutdown boundaries.
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn prepare_completed_history(
    processor: &crate::message::MessageProcessor,
    workspace: &str,
    thread: &str,
    turn: &str,
    current: &ModelSelection,
    settings: &CompactionSettings,
    cli_override: Option<&ModelSelection>,
    observer: Arc<dyn CompactionObserver>,
    cancellation: CancellationToken,
) -> Result<Option<String>> {
    super::history::prepare_history(
        &processor.crud_store.with_maintenance_access(),
        workspace,
        thread,
    )
    .await?;
    let mut diagnostic = HistoryCheckDiagnostic::default();
    let result = prepare_completed_history_owned(
        processor,
        workspace,
        thread,
        turn,
        current,
        settings,
        cli_override,
        observer,
        cancellation,
        None,
        None,
        0,
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        &mut diagnostic,
    )
    .await?;
    Ok(if result == HistoryCheckOutcome::Compacted {
        diagnostic.checkpoint
    } else {
        None
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn prepare_completed_history_owned(
    processor: &crate::message::MessageProcessor,
    workspace: &str,
    thread: &str,
    turn: &str,
    current: &ModelSelection,
    settings: &CompactionSettings,
    cli_override: Option<&ModelSelection>,
    observer: Arc<dyn CompactionObserver>,
    cancellation: CancellationToken,
    original_deadline: Option<u64>,
    target_output_cap: Option<u32>,
    fixed_input_tokens: u64,
    suspending: Arc<std::sync::atomic::AtomicBool>,
    diagnostic: &mut HistoryCheckDiagnostic,
) -> Result<HistoryCheckOutcome> {
    let store = processor.crud_store.with_maintenance_access();
    let clock: Arc<dyn CompactionClock> = Arc::new(SystemCompactionClock::default());
    let deadline = clock
        .now_ms()
        .saturating_add(pioneer_compaction::OPERATION_MILLIS)
        .min(original_deadline.unwrap_or(u64::MAX));
    diagnostic.stage = "executor".into();
    let Some(lease) = processor
        .compaction_coordinator
        .acquire(
            workspace,
            thread,
            super::ContextWorkPriority::Background,
            &cancellation,
        )
        .await?
    else {
        diagnostic.code = "executor_busy".into();
        return Ok(HistoryCheckOutcome::WaitingExecutor);
    };
    let cancellation = lease.cancellation();
    diagnostic.stage = "catalog".into();
    if pioneer_provider::catalog::model_catalog().is_err() {
        diagnostic.code = "catalog_unavailable".into();
        return Ok(HistoryCheckOutcome::WaitingCatalog);
    }
    diagnostic.stage = "history_preparation".into();
    // One resumable registration quantum. No incomplete projection is budgeted.
    let mut ready = false;
    for _ in 0..16 {
        ready = tokio::select! { biased;
            _ = cancellation.cancelled() => return Ok(HistoryCheckOutcome::Cancelled),
            _ = clock.sleep_until(deadline) => return Ok(HistoryCheckOutcome::Preparing),
            result = store.compaction_prepare_history_quantum(workspace, thread) => result?,
        };
        if ready {
            break;
        }
        tokio::task::yield_now().await;
    }
    if !ready {
        diagnostic.code = "history_preparation_incomplete".into();
        return Ok(HistoryCheckOutcome::Preparing);
    }
    let prepare = async {
        ensure!(
            store
                .compaction_turn_is_completed(workspace, thread, turn)
                .await?,
            "completed history preparation requires a completed scoped turn"
        );
        diagnostic.stage = "history_capture".into();
        let owner = super::native::native_owner(workspace, thread);
        let version = store
            .compaction_projection_version(workspace, thread)
            .await?;
        let json = processor
            .capture_current_context_basis(workspace, thread, turn, None)
            .await?;
        let allowed =
            super::frozen::accepted_history_scopes(&store, workspace, thread, &json).await?;
        let mut source_epochs = BTreeMap::new();
        for source_thread in &allowed {
            source_epochs.insert(
                source_thread.clone(),
                store
                    .compaction_projection_version(workspace, source_thread)
                    .await?,
            );
        }
        source_epochs.insert(thread.to_owned(), version);
        let mut messages =
            crate::turn_runtime_snapshot::restore_history_json(&store, workspace, &allowed, &json)
                .await?;
        let head = store.compaction_head(&owner).await?;
        let basis = if let Some(head) = &head {
            store
                .compaction_checkpoint_source(workspace, thread, head)
                .await?
                .map(|_| head.clone())
        } else {
            None
        };
        if let Some(basis) = &basis {
            super::checkpoint::project_checkpoint(
                &store,
                workspace,
                thread,
                &owner,
                basis,
                &allowed,
                &mut messages,
            )
            .await?;
        }
        diagnostic.stage = "target_configuration".into();
        let catalog = match current.transport {
            Transport::Codex => "openai-codex".to_owned(),
            Transport::Claude => "anthropic".to_owned(),
            Transport::Api => processor
                .provider_registry()
                .get_or_create_for_workspace(workspace, &current.instance)?
                .name()
                .to_owned(),
        };
        let limits = pioneer_provider::catalog::model_catalog()?.limits(&catalog, &current.model);
        let budget = ModelBudget::new(
            Some(limits.context_window),
            limits.max_input,
            limits.max_output,
        );
        // This estimates only the retained Pioneer history. It makes no claim
        // about hidden CLI instructions/tokens and never controls its context.
        // Every later native call still budgets its own complete request.
        let request = ChatRequest {
            model: current.model.clone(),
            messages,
            temperature: None,
            max_tokens: target_output_cap,
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        diagnostic.stage = "budget".into();
        let full = NativeRequestProjection::full(request, vec![], budget.clone(), false)?;
        diagnostic.estimated_input_tokens = Some(
            full.estimated_input_tokens
                .saturating_add(fixed_input_tokens),
        );
        diagnostic.padded_input_tokens = diagnostic
            .estimated_input_tokens
            .map(pioneer_compaction::padded_input);
        diagnostic.context_tokens = Some(budget.context);
        diagnostic.input_limit = budget.input_limit;
        diagnostic.output_reserve = Some(full.output_reserve);
        if budget.fits(
            full.estimated_input_tokens
                .saturating_add(fixed_input_tokens),
            full.output_reserve,
            false,
        ) {
            diagnostic.code = "history_fits".into();
            return Ok(None);
        }
        let selection =
            effective_selection(current, settings.selection.as_ref(), cli_override).clone();
        diagnostic.stage = "summary_configuration".into();
        let summarizer = super::service::make_summarizer(
            processor.provider_registry().as_ref(),
            Some(processor),
            workspace,
            selection,
        )
        .await?;
        diagnostic.stage = "planning".into();
        let layout = NativeHistoryLayout::from_messages(
            workspace,
            thread,
            &full.request.messages,
            &full.message_input_tokens,
        )?;
        let message_total: u64 = full
            .message_input_tokens
            .iter()
            .copied()
            .fold(0_u64, u64::saturating_add);
        let fixed = full
            .estimated_input_tokens
            .saturating_sub(message_total)
            .saturating_add(fixed_input_tokens);
        let available = budget
            .context
            .saturating_sub(full.output_reserve)
            .saturating_sub(fixed);
        let goal = summarizer
            .model_budget()
            .summarizer_cap(u64::MAX)?
            .min(available / 2)
            .max(1);
        let mut plan = plan_compaction(
            &layout.units,
            &budget,
            full.output_reserve,
            fixed,
            goal,
            CompactionMode::Normal,
            false,
            &format!("{owner}:{version}:{head:?}"),
        )
        .or_else(|_| {
            plan_compaction(
                &layout.units,
                &budget,
                full.output_reserve,
                fixed,
                goal,
                CompactionMode::Emergency,
                false,
                &format!("{owner}:{version}:{head:?}"),
            )
        })
        .map_err(|error| {
            anyhow::anyhow!("completed history has no fitting whole-unit plan: {error:?}")
        })?;
        for (index, unit) in layout.units.iter().enumerate() {
            if unit.role == SourceRole::Own
                && unit.sources.iter().any(|source| {
                    source.scope == format!("checkpoint:{owner}")
                        && basis.as_ref() == Some(&source.id)
                })
            {
                if !plan.compact.contains(&index) {
                    plan.compact.push(index);
                }
                plan.retain.retain(|other| *other != index);
            }
        }
        plan.compact.sort_unstable();
        plan.coverage = plan
            .compact
            .iter()
            .flat_map(|index| layout.units[*index].sources.clone())
            .collect();
        let indexes: BTreeSet<_> = plan
            .compact
            .iter()
            .flat_map(|index| layout.message_indexes[*index].iter().copied())
            .collect();
        ensure!(
            !indexes.is_empty(),
            "completed history compaction selected no complete work"
        );
        let target_identity = hex::encode(Sha256::digest(serde_json::to_vec(&(
            current,
            &budget,
            full.output_reserve,
            fixed_input_tokens,
            &full.request.messages,
        ))?));
        let projection = BackgroundHistoryTarget {
            projection: NativeRequestProjection::new(
                full.request,
                indexes,
                vec![],
                budget.clone(),
                false,
            )?,
            budget,
            fixed_input_tokens,
        };
        let mut manifest = Vec::new();
        for (reference_only, units) in [(false, &plan.compact), (true, &plan.retain)] {
            for unit in units {
                for source in &layout.units[*unit].sources {
                    let source_thread = layout.source_threads.get(source).ok_or_else(|| {
                        anyhow::anyhow!("completed history source owner is missing")
                    })?;
                    manifest.push(ManifestEntry {
                        ordinal: manifest.len() as u64,
                        unit: *unit as u64,
                        reference_only,
                        thread_id: source_thread.clone(),
                        source: source.clone(),
                    });
                }
            }
        }
        diagnostic.stage = "admission".into();
        let snapshot = admit_operation(
            &store,
            workspace,
            thread,
            settings,
            current,
            cli_override,
            summarizer.as_ref(),
            PreparedOperation {
                owner,
                execution_turn: turn.into(),
                source_projection: Some(serde_json::from_str(&json)?),
                expected_checkpoint: head,
                summary_basis: basis,
                operation_deadline_ms: Some(deadline),
                projection_version: version,
                source_epochs,
                plan,
                manifest,
                target_identity,
                target_tokens: goal,
            },
            clock.now_ms(),
        )
        .await?;
        Ok::<_, anyhow::Error>(Some((snapshot, summarizer, projection)))
    };
    let operation = tokio::select! { biased;
        _ = cancellation.cancelled() => return Ok(HistoryCheckOutcome::Cancelled),
        _ = clock.sleep_until(deadline) => return Err(HistoryCheckDeadline.into()),
        result = prepare => result?,
    };
    let Some((snapshot, summarizer, projection)) = operation else {
        return Ok(HistoryCheckOutcome::Fits);
    };
    diagnostic.stage = "summarization".into();
    diagnostic.operation = Some(snapshot.id.clone());
    let operation_deadline = deadline.min(snapshot.admission.deadline_ms);
    let runner = CompactionRunner::new(
        store,
        workspace.into(),
        thread.into(),
        snapshot,
        summarizer,
        Arc::new(projection),
        observer,
        clock.clone(),
    );
    let exit = tokio::select! { biased;
        _ = clock.sleep_until(operation_deadline) => CompactionExit::Reconcile(FailureKind::Deadline),
        result = runner.run(cancellation) => result?,
    };
    if suspending.load(std::sync::atomic::Ordering::Acquire)
        && matches!(exit, CompactionExit::Reconcile(FailureKind::Cancelled))
    {
        // run has joined service cleanup. Keep the persisted attempt and the
        // original deadline; startup will recover it using a fresh CLI context.
        anyhow::bail!("completed history preparation suspended for shutdown");
    }
    let exit = match exit {
        CompactionExit::Reconcile(reason) => runner.reconcile(reason).await?,
        exit => exit,
    };
    match exit {
        CompactionExit::Applied(checkpoint) => {
            diagnostic.code = "summary_applied".into();
            diagnostic.checkpoint = Some(checkpoint);
            Ok(HistoryCheckOutcome::Compacted)
        }
        CompactionExit::Failed(reason) | CompactionExit::Reconcile(reason) => {
            diagnostic.code = match reason {
                FailureKind::Transient => "provider_retries_exhausted",
                FailureKind::Permanent => "provider_permanent_failure",
                FailureKind::InvalidCompletion => "invalid_completion",
                FailureKind::InsufficientEffect => "insufficient_effect",
                FailureKind::Deadline => "operation_deadline",
                FailureKind::Cancelled => "cancelled",
            }
            .into();
            Ok(if reason == FailureKind::Cancelled {
                HistoryCheckOutcome::Cancelled
            } else {
                HistoryCheckOutcome::Failed
            })
        }
    }
}

struct BackgroundHistoryTarget {
    projection: NativeRequestProjection,
    budget: ModelBudget,
    fixed_input_tokens: u64,
}
#[async_trait::async_trait]
impl CompactionTarget for BackgroundHistoryTarget {
    async fn fits(&self, summary: &str) -> Result<bool> {
        let evaluated = self.projection.evaluate(summary)?;
        Ok(self.budget.fits(
            evaluated
                .estimated_input_tokens
                .saturating_add(self.fixed_input_tokens),
            evaluated.output_reserve,
            false,
        ))
    }
}
