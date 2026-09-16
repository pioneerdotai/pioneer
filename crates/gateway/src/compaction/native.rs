//! Native request preparation. This function owns admission and the service
//! operation; callers retain the turn control scope through reconciliation.
use super::*;
use pioneer_agent::compaction::{
    controller::{
        NativeContext, NativeInputReceipt, NativePreparedRequest, NativeUsageMeasurement,
    },
    history::NativeHistoryLayout,
    request::NativeRequestProjection,
};
use pioneer_compaction::{
    CompactionMode, CompactionSettings, ModelBudget, ModelSelection, SourceRole, Transport,
    effective_selection, plan_compaction,
};
use pioneer_crud::compaction::ManifestEntry;
use pioneer_provider::{ChatRequest, MessageProvenance, MessageSourceRef, ProviderRegistry};
use std::collections::BTreeSet;

pub(crate) fn native_owner(workspace: &str, thread: &str) -> String {
    // Lengths disambiguate arbitrary scope identifiers without parsing them.
    format!(
        "native:{}:{workspace}:{}:{thread}",
        workspace.len(),
        thread.len()
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn prepare_native_projection(
    store: &CrudStore,
    providers: &ProviderRegistry,
    settings: &CompactionSettings,
    context: &NativeContext,
    mut request: ChatRequest,
    measured: Option<NativeUsageMeasurement>,
    recovery: bool,
    observer: Arc<dyn CompactionObserver>,
    clock: Arc<dyn CompactionClock>,
    accepted_projection: Option<pioneer_compaction::frozen::FrozenHistoryRef>,
    processor: Option<&crate::message::MessageProcessor>,
) -> Result<NativePreparedRequest> {
    let store = store.with_maintenance_access();
    let workspace = context.workspace_id.as_str();
    let thread = context.thread_id.as_str();
    let owner = native_owner(workspace, thread);
    let now = clock.now_ms();
    let operation_deadline = context.recovery_deadline_ms;
    let preparation_deadline = now
        .saturating_add(pioneer_compaction::OPERATION_MILLIS)
        .min(operation_deadline.unwrap_or(u64::MAX));
    // This deadline includes reader/writer queues and admission, not just LLM time.
    let prepared = async {
        let catalog = pioneer_provider::catalog::model_catalog()?;
        let version = store
            .compaction_projection_version(workspace, thread)
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
        let mut authorized = super::frozen::execution_history_scopes(
            &store,
            workspace,
            thread,
            &context.turn_id,
            context.conversation_thread_id.as_deref(),
        )
        .await?;
        if let Some(projection) = &accepted_projection {
            authorized.extend(
                super::frozen::accepted_history_scopes(
                    &store,
                    workspace,
                    thread,
                    &serde_json::to_string(projection)?,
                )
                .await?,
            );
        }
        let source_projection = if accepted_projection.is_some() {
            accepted_projection
        } else {
            store
                .compaction_task_basis_snapshot(workspace, thread, &context.turn_id)
                .await?
                .filter(|basis| !basis.history_json.trim_start().starts_with('['))
                .map(|basis| serde_json::from_str(&basis.history_json))
                .transpose()?
        };
        let mut source_epochs = std::collections::BTreeMap::from([(thread.to_owned(), version)]);
        // The parent scope comes from the Gateway's accepted execution snapshot,
        // not model-provided text or an arbitrary source reference.
        for parent in authorized.iter().filter(|parent| parent.as_str() != thread) {
            let parent_epoch = store
                .compaction_projection_version(workspace, parent)
                .await?;
            source_epochs.insert(parent.clone(), parent_epoch);
        }
        super::origins::resolve_message_origins(
            &store,
            workspace,
            thread,
            &context.turn_id,
            &authorized,
            &mut request.messages,
        )
        .await?;
        if let Some(basis) = &basis {
            super::checkpoint::project_checkpoint(
                &store,
                workspace,
                thread,
                &owner,
                basis,
                &authorized,
                &mut request.messages,
            )
            .await?;
        }
        super::checkpoint::project_accepted_checkpoints(
            &store,
            workspace,
            thread,
            &authorized,
            &mut request.messages,
        )
        .await?;
        let limits = catalog.limits(context.provider.name(), &request.model);
        let budget = ModelBudget::new(
            Some(limits.context_window),
            limits.max_input,
            limits.max_output,
        );
        // Resolve and pin media under the selected provider authority before
        // evaluating either the full request or any compaction candidate.
        let materialized = context.provider.prepare_input_budget(request).await?;
        let media = materialized.media;
        let mut full = NativeRequestProjection::full(
            materialized.request,
            media.clone(),
            budget.clone(),
            recovery,
        )?;
        let api_identity = format!(
            "{}:{}",
            context.provider.name(),
            context
                .provider
                .authority_fingerprint()
                .unwrap_or("unbound")
        );
        let mut receipt = NativeInputReceipt::for_request(
            &full.request,
            &context.provider_instance,
            &api_identity,
            version,
            basis.clone(),
        )?;
        let mut message_tokens = full.message_input_tokens.clone();
        let mut input = receipt.calibrated_input(
            full.estimated_input_tokens,
            &message_tokens,
            measured.as_ref(),
        )?;
        if !budget.fits(input, full.output_reserve, recovery) {
            if let Some(reduced) = super::result_budget::shrink_results(
                &store, workspace, &full, &media, &budget, recovery,
            )
            .await?
            {
                full = reduced;
                message_tokens = full.message_input_tokens.clone();
                receipt = NativeInputReceipt::for_request(
                    &full.request,
                    &context.provider_instance,
                    &api_identity,
                    version,
                    basis.clone(),
                )?;
                // Representation changed: exact-prefix validation decides
                // whether any earlier provider measurement is still usable.
                input = receipt.calibrated_input(
                    full.estimated_input_tokens,
                    &message_tokens,
                    measured.as_ref(),
                )?;
            }
        }
        if !recovery && budget.fits(input, full.output_reserve, false) {
            return Ok::<_, anyhow::Error>((full.request, receipt, None));
        }
        let _startup_compaction = pioneer_observability::turn_startup::current_stage(
            pioneer_observability::turn_startup::Stage::CompactionWork,
        );
        let effort = match full.request.reasoning {
            Some(pioneer_provider::ReasoningConfig::Effort(effort)) => {
                Some(effort.as_str().to_owned())
            }
            Some(pioneer_provider::ReasoningConfig::Disabled) => Some("none".into()),
            None => None,
        };
        let current = ModelSelection {
            transport: Transport::Api,
            instance: context.provider_instance.clone(),
            model: full.request.model.clone(),
            effort,
        };
        let selection = effective_selection(&current, settings.selection.as_ref(), None).clone();
        let summarizer =
            super::service::make_summarizer(providers, processor, workspace, selection).await?;
        let layout = NativeHistoryLayout::from_messages(
            workspace,
            thread,
            &full.request.messages,
            &message_tokens,
        )?;
        let message_total = message_tokens
            .iter()
            .fold(0_u64, |sum, n| sum.saturating_add(*n));
        let fixed = input.saturating_sub(message_total);
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
            recovery,
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
                recovery,
                &format!("{owner}:{version}:{head:?}"),
            )
        })
        .map_err(|e| {
            anyhow::anyhow!("context has no fitting whole-round compaction plan: {e:?}")
        })?;
        // A new summary incorporates the old basis. Remove its old prompt slot
        // as part of the same replacement, even if the tail planner retained it.
        for (index, unit) in layout.units.iter().enumerate() {
            if unit.role == SourceRole::Own
                && unit.sources.iter().any(|s| {
                    s.scope == format!("checkpoint:{owner}") && basis.as_ref() == Some(&s.id)
                })
            {
                if !plan.compact.contains(&index) {
                    plan.compact.push(index);
                }
                plan.retain.retain(|i| *i != index);
            }
        }
        plan.compact.sort_unstable();
        plan.coverage = plan
            .compact
            .iter()
            .flat_map(|i| layout.units[*i].sources.clone())
            .collect();
        let indexes: BTreeSet<_> = plan
            .compact
            .iter()
            .flat_map(|i| layout.message_indexes[*i].iter().copied())
            .collect();
        let first = *indexes
            .first()
            .ok_or_else(|| anyhow::anyhow!("empty native compaction plan"))?;
        let projection =
            NativeRequestProjection::new(full.request.clone(), indexes, media, budget, recovery)?;
        let mut manifest = Vec::new();
        for (reference_only, units) in [(false, &plan.compact), (true, &plan.retain)] {
            for unit in units {
                for source in &layout.units[*unit].sources {
                    if let Some(thread_id) = layout.source_threads.get(source) {
                        manifest.push(ManifestEntry {
                            ordinal: manifest.len() as u64,
                            unit: *unit as u64,
                            reference_only,
                            thread_id: thread_id.clone(),
                            source: source.clone(),
                        });
                    }
                }
            }
        }
        let snapshot = admit_operation(
            &store,
            workspace,
            thread,
            settings,
            &current,
            None,
            summarizer.as_ref(),
            PreparedOperation {
                owner: owner.clone(),
                execution_turn: context.turn_id.clone(),
                source_projection,
                expected_checkpoint: head,
                summary_basis: basis,
                operation_deadline_ms: operation_deadline,
                projection_version: version,
                source_epochs,
                plan,
                manifest,
                target_identity: receipt.target_fingerprint()?,
                target_tokens: goal,
            },
            now,
        )
        .await?;
        Ok((
            full.request,
            receipt,
            Some((snapshot, summarizer, projection, first)),
        ))
    };
    let (request, receipt, operation) = tokio::select! { biased;
        _ = context.cancellation.cancelled() => anyhow::bail!("native context preparation cancelled"),
        _ = clock.sleep_until(preparation_deadline) => anyhow::bail!("native context preparation deadline exceeded"),
        prepared = prepared => prepared?,
    };
    let Some((snapshot, summarizer, projection, summary_index)) = operation else {
        return Ok(NativePreparedRequest { request, receipt });
    };
    let request_deadline = preparation_deadline.min(snapshot.admission.deadline_ms);
    let runner = CompactionRunner::new(
        store.clone(),
        workspace.into(),
        thread.into(),
        snapshot,
        summarizer,
        Arc::new(NativeRequestTarget(projection.clone())),
        observer,
        clock.clone(),
    );
    let execution = tokio::select! { biased;
        _ = clock.sleep_until(request_deadline) => CompactionExit::Reconcile(FailureKind::Deadline),
        result = runner.run(context.cancellation.clone()) => result?,
    };
    let outcome = match execution {
        CompactionExit::Reconcile(reason) => runner.reconcile(reason).await?,
        outcome => outcome,
    };
    let CompactionExit::Applied(id) = outcome else {
        anyhow::bail!("native context compaction did not apply: {outcome:?}");
    };
    // Publishing the checkpoint does not grant a new budget to re-read and
    // materialize the main request. A late Stop/deadline keeps the checkpoint
    // durable, but prevents a new provider call.
    let materialize = async {
        let checkpoint = store
            .compaction_checkpoint(&id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("applied summary missing"))?;
        let source = store
            .compaction_checkpoint_source(workspace, thread, &id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("applied summary was invalidated"))?;
        let mut evaluated = projection.evaluate(&checkpoint.summary)?;
        ensure!(
            evaluated.fits,
            "applied summary no longer fits complete native request"
        );
        evaluated.request.messages[summary_index].provenance = Some(MessageProvenance {
            logical_turn_id: None,
            workspace_id: workspace.into(),
            thread_id: thread.into(),
            context_thread: None,
            unit_id: format!("checkpoint:{owner}"),
            sources: vec![MessageSourceRef {
                scope: source.scope,
                id: source.id,
                version: source.version,
            }],
            complete: true,
            protected_input: false,
            inherited: false,
        });
        let receipt = NativeInputReceipt::for_request(
            &evaluated.request,
            &context.provider_instance,
            &receipt.identity.api_format,
            checkpoint.projection_version,
            Some(id),
        )?;
        Ok(NativePreparedRequest {
            request: evaluated.request,
            receipt,
        })
    };
    tokio::select! { biased;
        _ = context.cancellation.cancelled() => anyhow::bail!("native context preparation cancelled after checkpoint"),
        _ = clock.sleep_until(request_deadline) => anyhow::bail!("native context preparation deadline exceeded after checkpoint"),
        result = materialize => result,
    }
}

/// Gateway owns settings, durable source projection and service transports.
/// The agent loop owns this controller's per-turn/background future.
pub(crate) struct GatewayNativeContextController {
    processor: std::sync::Weak<crate::message::MessageProcessor>,
}
impl GatewayNativeContextController {
    pub(crate) fn new(processor: std::sync::Weak<crate::message::MessageProcessor>) -> Self {
        Self { processor }
    }
    fn observer(&self, context: &NativeContext) -> Arc<dyn CompactionObserver> {
        Arc::new(HubCompactionObserver {
            processor: self.processor.clone(),
            hub: context.events.clone(),
            workspace: context.workspace_id.clone(),
            thread: context.thread_id.clone(),
            turn: context.turn_id.clone(),
        })
    }
}
#[async_trait]
impl pioneer_agent::compaction::controller::NativeContextController
    for GatewayNativeContextController
{
    async fn stop(&self, context: &NativeContext) -> Result<()> {
        let processor = self
            .processor
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("context owner stopped"))?;
        processor
            .crud_store
            .compaction_stop_execution(
                &context.workspace_id,
                &context.thread_id,
                &native_owner(&context.workspace_id, &context.thread_id),
                &context.turn_id,
            )
            .await?;
        processor
            .stop_completed_history_check(
                &context.workspace_id,
                &context.thread_id,
                &context.turn_id,
            )
            .await;
        Ok(())
    }

    async fn prepare(
        &self,
        context: &NativeContext,
        request: ChatRequest,
        measurement: Option<NativeUsageMeasurement>,
        recovery: bool,
    ) -> Result<NativePreparedRequest> {
        let processor = self
            .processor
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("context owner stopped"))?;
        let clock: Arc<dyn CompactionClock> = Arc::new(SystemCompactionClock::default());
        let deadline = clock
            .now_ms()
            .saturating_add(pioneer_compaction::OPERATION_MILLIS)
            .min(context.recovery_deadline_ms.unwrap_or(u64::MAX));
        let startup_wait = pioneer_observability::turn_startup::stage(
            &context.turn_id,
            pioneer_observability::turn_startup::Stage::CompactionWait,
        );
        let lease = tokio::select! { biased;
            _ = context.cancellation.cancelled() => anyhow::bail!("native context preparation cancelled"),
            _ = clock.sleep_until(deadline) => anyhow::bail!("native context preparation deadline exceeded"),
            result = processor.compaction_coordinator.acquire(
                &context.workspace_id, &context.thread_id,
                super::ContextWorkPriority::Foreground, &context.cancellation,
            ) => result?.ok_or_else(|| anyhow::anyhow!("native context owner unavailable"))?,
        };
        drop(startup_wait);
        let refresh = refresh_native_history(&processor, context, request);
        let (request, projection) = tokio::select! { biased;
            _ = context.cancellation.cancelled() => anyhow::bail!("native context preparation cancelled"),
            _ = clock.sleep_until(deadline) => anyhow::bail!("native context preparation deadline exceeded"),
            result = refresh => result?,
        };
        let mut context = context.clone();
        context.recovery_deadline_ms = Some(deadline);
        context.cancellation = lease.cancellation();
        prepare_native_projection(
            processor.crud_store.as_ref(),
            processor.provider_registry().as_ref(),
            &processor.compaction_settings_for_workspace(&context.workspace_id)?,
            &context,
            request,
            measurement,
            recovery,
            self.observer(&context),
            clock,
            Some(projection),
            Some(&processor),
        )
        .await
    }
    async fn after_turn(
        &self,
        context: &NativeContext,
        request: ChatRequest,
        _measurement: Option<NativeUsageMeasurement>,
    ) -> Result<()> {
        let processor = self
            .processor
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("context owner stopped"))?;
        // This durable intent prepares retained history for a later turn. The
        // foreground prepare path above always validates the full actual
        // request, including its current instructions/tools/media and reserve.
        processor
            .enqueue_native_completed_history(context, request)
            .await
    }
}

/// Refresh completed history while retaining the current execution's exact
/// input and unfinished tool rounds. Mixed checkpoints are expanded through
/// their canonical coverage before selecting the current-turn suffix.
async fn refresh_native_history(
    processor: &crate::message::MessageProcessor,
    context: &NativeContext,
    mut request: ChatRequest,
) -> Result<(ChatRequest, pioneer_compaction::frozen::FrozenHistoryRef)> {
    use pioneer_agent::compaction::composition::ScopedHistorySource;
    let store = processor.crud_store.with_maintenance_access();
    let json = processor
        .capture_current_context_basis(
            &context.workspace_id,
            &context.thread_id,
            &context.turn_id,
            Some(&context.turn_id),
        )
        .await?;
    let allowed = super::frozen::accepted_history_scopes(
        &store,
        &context.workspace_id,
        &context.thread_id,
        &json,
    )
    .await?;
    let mut history = crate::turn_runtime_snapshot::restore_history_json(
        &store,
        &context.workspace_id,
        &allowed,
        &json,
    )
    .await?;
    let is_current = |thread: &str, scope: &str| {
        thread == context.thread_id
            && scope.split_once(':').is_some_and(|(kind, turn)| {
                turn == context.turn_id
                    && matches!(
                        kind,
                        "input"
                            | "event"
                            | "context"
                            | "item"
                            | "pending-input"
                            | "pending-assistant"
                            | "pending-tool"
                            | "pending-item"
                    )
            })
    };
    for message in request.messages {
        let Some(origin) = &message.provenance else {
            // Current runtime instructions without canonical history provenance
            // remain protected by the request planner.
            history.push(message);
            continue;
        };
        let checkpoint = origin
            .sources
            .iter()
            .find(|source| source.scope.starts_with("checkpoint:"));
        let expanded = if let Some(checkpoint) = checkpoint {
            let source = SourceRef {
                scope: checkpoint.scope.clone(),
                id: checkpoint.id.clone(),
                version: checkpoint.version.clone(),
            };
            let leaves = super::coverage::checkpoint_leaves(
                &store,
                &context.workspace_id,
                &allowed,
                &source,
            )
            .await?;
            if !leaves
                .iter()
                .any(|leaf| is_current(&leaf.thread, &leaf.source.scope))
            {
                continue;
            }
            let checkpoints = std::collections::BTreeMap::from([(
                ScopedHistorySource {
                    thread: origin.thread_id.clone(),
                    source,
                },
                leaves.clone(),
            )]);
            super::compatible::rematerialize_overlap(
                &store,
                &context.workspace_id,
                &allowed,
                &[message],
                &checkpoints,
                &leaves,
            )
            .await?
        } else {
            vec![message]
        };
        for mut message in expanded {
            let origin = message
                .provenance
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("current history lost provenance"))?;
            if origin
                .sources
                .iter()
                .any(|source| is_current(&origin.thread_id, &source.scope))
            {
                ensure!(
                    origin
                        .sources
                        .iter()
                        .all(|source| is_current(&origin.thread_id, &source.scope)),
                    "history unit crosses the current execution boundary"
                );
                if origin.sources.iter().any(|source| {
                    source.scope.starts_with("input:") || source.scope.starts_with("pending-input:")
                }) {
                    origin.protected_input = true;
                }
                history.push(message);
            }
        }
    }
    request.messages = history;
    Ok((request, serde_json::from_str(&json)?))
}

/// Completed canonical history replaces conversational messages, while the
/// target request retains its system instructions and all non-message fields.
#[cfg(test)]
fn replace_completed_history(
    request: &mut ChatRequest,
    messages: Vec<pioneer_provider::ChatMessage>,
) {
    request
        .messages
        .retain(|message| message.role == pioneer_provider::Role::System);
    request.messages.extend(messages);
}

#[cfg(test)]
mod completed_request_tests {
    use super::*;
    use pioneer_provider::ChatMessage;

    #[test]
    fn completed_history_keeps_system_instructions_in_full_request_budget() {
        let instructions = "Required runtime instruction. ".repeat(500);
        let mut request = ChatRequest {
            model: "fixture".into(),
            messages: vec![
                ChatMessage::system(instructions.clone()),
                ChatMessage::user("pending input"),
            ],
            temperature: None,
            max_tokens: Some(512),
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        replace_completed_history(
            &mut request,
            vec![
                ChatMessage::user("accepted input"),
                ChatMessage::assistant("completed answer"),
            ],
        );
        assert_eq!(request.messages.len(), 3);
        assert_eq!(request.messages[0].content, instructions);
        assert_eq!(request.messages[1].content, "accepted input");
        let budget = ModelBudget::new(Some(2048), None, Some(512));
        let complete =
            NativeRequestProjection::full(request.clone(), vec![], budget.clone(), false).unwrap();
        assert!(
            !complete.fits,
            "system instructions must participate in the post-turn budget"
        );
        request.messages.remove(0);
        assert!(
            NativeRequestProjection::full(request, vec![], budget, false)
                .unwrap()
                .fits
        );
    }
}
