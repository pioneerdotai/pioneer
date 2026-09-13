//! Prepare completed native or completed history for later Pioneer continuations.
//! Every actual native model call independently budgets its complete request.
use super::*;
use pioneer_agent::compaction::{history::NativeHistoryLayout, request::NativeRequestProjection};
use pioneer_compaction::{
    CompactionMode, CompactionSettings, ModelBudget, ModelSelection, SourceRole, Transport,
    effective_selection, plan_compaction,
};
use pioneer_crud::compaction::ManifestEntry;
use pioneer_provider::ChatRequest;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

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
    prepare_completed_history_owned(
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
    )
    .await
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
) -> Result<Option<String>> {
    let store = processor.crud_store.with_maintenance_access();
    let clock: Arc<dyn CompactionClock> = Arc::new(SystemCompactionClock::default());
    let deadline = clock
        .now_ms()
        .saturating_add(pioneer_compaction::OPERATION_MILLIS)
        .min(original_deadline.unwrap_or(u64::MAX));
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
        return Ok(None);
    };
    let cancellation = lease.cancellation();
    let prepare = async {
        ensure!(
            store
                .compaction_turn_is_completed(workspace, thread, turn)
                .await?,
            "completed history preparation requires a completed scoped turn"
        );
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
        let full = NativeRequestProjection::full(request, vec![], budget.clone(), false)?;
        if budget.fits(
            full.estimated_input_tokens
                .saturating_add(fixed_input_tokens),
            full.output_reserve,
            false,
        ) {
            return Ok(None);
        }
        let selection =
            effective_selection(current, settings.selection.as_ref(), cli_override).clone();
        let summarizer = super::service::make_summarizer(
            processor.provider_registry().as_ref(),
            Some(processor),
            workspace,
            selection,
        )
        .await?;
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
        _ = cancellation.cancelled() => anyhow::bail!("completed history preparation cancelled"),
        _ = clock.sleep_until(deadline) => anyhow::bail!("completed history preparation deadline exceeded"),
        result = prepare => result?,
    };
    let Some((snapshot, summarizer, projection)) = operation else {
        return Ok(None);
    };
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
        CompactionExit::Applied(checkpoint) => Ok(Some(checkpoint)),
        exit => anyhow::bail!("completed history preparation did not apply: {exit:?}"),
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
