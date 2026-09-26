//! Admission/resume uses bounded metadata and source reads; model projection
//! runs only after each database read has released its capacity.
use super::*;
use pioneer_agent::compaction::history::NativeHistoryLayout;
use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenReplayEdge};
use pioneer_compaction::{
    CompactionPlan, CompactionSettings, ModelBudget, ModelSelection, SourceRef,
    coverage_domain_for, effective_selection,
};
use pioneer_crud::compaction::{ManifestEntry, SOURCE_PAGE_BYTES};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

/// Limit only aliases that would be published by this checkpoint. Frozen
/// histories can combine independently accepted branches with many more exact
/// aliases; retained and reference-only sources do not enter checkpoint edges.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn fit_checkpoint_replay_aliases(
    store: &CrudStore,
    workspace: &str,
    owner_thread: &str,
    descriptor: Option<&FrozenHistoryRef>,
    layout: &NativeHistoryLayout,
    plan: &mut CompactionPlan,
    budget: &ModelBudget,
    reserve: u64,
    fixed_input: u64,
    summary_goal: u64,
    recovery: bool,
    required_checkpoint: Option<&str>,
) -> Result<()> {
    let Some(descriptor) = descriptor else {
        return Ok(());
    };
    ensure!(
        store
            .compaction_frozen_history_owner(workspace, descriptor)
            .await?
            .as_deref()
            == Some(owner_thread),
        "compaction source projection owner changed"
    );
    let mut edges_by_source =
        std::collections::BTreeMap::<SourceRef, BTreeSet<FrozenReplayEdge>>::new();
    let mut ordinal = 0_u64;
    while ordinal < descriptor.messages {
        let page = store
            .compaction_frozen_history_page(
                workspace,
                owner_thread,
                &descriptor.manifest_id,
                ordinal,
            )
            .await?;
        ensure!(
            !page.is_empty(),
            "compaction source projection lost a reference page"
        );
        for reference in page {
            reference.validate()?;
            for source in &reference.sources {
                if layout.source_threads.get(source).map(String::as_str)
                    != Some(reference.source_thread.as_str())
                {
                    continue;
                }
                edges_by_source
                    .entry(source.clone())
                    .or_default()
                    .extend(reference.publication_edges_for(source));
            }
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("frozen ordinal overflow"))?;
            ensure!(
                ordinal <= descriptor.messages,
                "compaction source projection count mismatch"
            );
        }
    }
    let selected_edges = |compact: &[usize]| {
        compact
            .iter()
            .flat_map(|index| &layout.units[*index].sources)
            .filter_map(|source| edges_by_source.get(source))
            .flat_map(BTreeSet::iter)
            .collect::<BTreeSet<_>>()
            .len()
    };
    while selected_edges(&plan.compact) > pioneer_compaction::REPLAY_ALIAS_LIMIT {
        let mut choice = None;
        for index in plan.compact.iter().copied() {
            let unit = &layout.units[index];
            if unit.sources.iter().any(|source| {
                source.scope.starts_with("checkpoint:")
                    && required_checkpoint == Some(source.id.as_str())
            }) {
                continue;
            }
            let remaining = plan
                .compact
                .iter()
                .copied()
                .filter(|other| *other != index)
                .collect::<Vec<_>>();
            let remaining_aliases = selected_edges(&remaining);
            if remaining_aliases >= selected_edges(&plan.compact) || remaining.is_empty() {
                continue;
            }
            let retained_tokens = plan.retain.iter().chain(std::iter::once(&index)).fold(
                fixed_input.saturating_add(summary_goal),
                |total, retained| total.saturating_add(layout.units[*retained].tokens),
            );
            if budget.fits(retained_tokens, reserve, recovery) {
                let candidate = (remaining_aliases, unit.tokens, index);
                if choice.is_none_or(|best| candidate < best) {
                    choice = Some(candidate);
                }
            }
        }
        let (_, _, index) = choice.ok_or_else(|| {
            anyhow::anyhow!("no fitting whole-round plan within checkpoint replay alias limit")
        })?;
        plan.compact.retain(|selected| *selected != index);
        plan.retain.push(index);
        plan.retain.sort_unstable();
    }
    plan.coverage_domain = coverage_domain_for(&layout.units, &plan.compact);
    plan.coverage = plan
        .compact
        .iter()
        .flat_map(|index| layout.units[*index].sources.clone())
        .collect();
    Ok(())
}

#[derive(Clone)]
pub(crate) struct PreparedOperation {
    pub owner: String,
    pub execution_turn: String,
    pub source_projection: Option<pioneer_compaction::frozen::FrozenHistoryRef>,
    pub expected_checkpoint: Option<String>,
    /// Published checkpoint selected as the atomic previous-summary input.
    /// Historical leaf edits do not make this basis stale.
    pub summary_basis: Option<String>,
    pub operation_deadline_ms: Option<u64>,
    pub projection_version: u64,
    pub source_epochs: std::collections::BTreeMap<String, u64>,
    pub plan: CompactionPlan,
    pub manifest: Vec<ManifestEntry>,
    /// Includes the main model, instructions/tools/media/output budget and access
    /// scope. A changed target may retry a previously ineffective source plan.
    pub target_identity: String,
    pub target_tokens: u64,
}

/// The caller owns this future, including cancellation during admission. A crash
/// leaves an incomplete manifest resumable, never eligible for provider calls.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn admit_operation(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    settings: &CompactionSettings,
    current: &ModelSelection,
    cli_override: Option<&ModelSelection>,
    summarizer: &dyn Summarizer,
    mut prepared: PreparedOperation,
    now_ms: u64,
) -> Result<OperationSnapshot> {
    // Filter the canonical manifest before fingerprinting, budgeting or
    // publishing an operation. Older cached classifications can still name a
    // source whose current typed model projection is empty.
    let mut manifest = Vec::with_capacity(prepared.manifest.len());
    for mut entry in prepared.manifest {
        if entry.source.scope.starts_with("event:") || entry.source.scope.starts_with("item:") {
            store
                .compaction_prepare_references(
                    workspace,
                    &entry.thread_id,
                    std::slice::from_ref(&entry.source),
                )
                .await?;
            let payload = store
                .compaction_reference_payload(workspace, &entry.thread_id, &entry.source)
                .await?
                .ok_or_else(|| anyhow::anyhow!("compaction source unavailable during admission"))?;
            if super::model_source_payload(&entry.source, payload)?.is_none() {
                continue;
            }
        }
        entry.ordinal = manifest.len() as u64;
        manifest.push(entry);
    }
    prepared.manifest = manifest;
    let selection = effective_selection(current, settings.selection.as_ref(), cli_override);
    let budget = summarizer.model_budget();
    // Fresh captures of the same accepted history have different storage IDs.
    // Retry identity is its immutable content and import proof, never that ID
    // or a newly computed wall-clock deadline.
    let projection_identity = if let Some(projection) = &prepared.source_projection {
        let owner = store
            .compaction_frozen_history_owner(workspace, projection)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source projection is unavailable"))?;
        let imports = store
            .compaction_frozen_import_state(workspace, &owner, &projection.manifest_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source projection import proof is unavailable"))?;
        Some((
            owner,
            projection.format,
            projection.identity_sha256.clone(),
            projection.messages,
            imports,
        ))
    } else {
        None
    };
    let mut identity = Sha256::new();
    identity.update(serde_json::to_vec(&(
        &prepared.owner,
        &prepared.expected_checkpoint,
        &prepared.summary_basis,
        prepared.projection_version,
        &prepared.source_epochs,
        &projection_identity,
        &prepared.plan.fingerprint,
        &prepared.target_identity,
        pioneer_protocol::HISTORICAL_COMMAND_LLM_PROJECTION_VERSION,
        selection,
        &budget,
        prepared.target_tokens,
    ))?);
    let mut seen = BTreeSet::new();
    let mut selected = 0_u64;
    for (ordinal, entry) in prepared.manifest.iter().enumerate() {
        ensure!(entry.ordinal == ordinal as u64, "non-contiguous manifest");
        ensure!(
            seen.insert(entry.source.clone()),
            "duplicate manifest source"
        );
        selected += u64::from(!entry.reference_only);
        identity.update(serde_json::to_vec(&(
            entry.ordinal,
            entry.unit,
            entry.reference_only,
            &entry.thread_id,
            &entry.source,
        ))?);
    }
    ensure!(selected > 0, "no selected compaction material");
    let fingerprint = hex::encode(identity.finalize());
    let existing = store
        .compaction_operation_for_plan(workspace, thread, &prepared.owner, &fingerprint)
        .await?;
    let mut record = if let Some(record) = existing {
        record
    } else {
        let mut admission = settings.admit(current, cli_override, now_ms)?;
        if let Some(deadline) = prepared.operation_deadline_ms {
            admission.deadline_ms = admission.deadline_ms.min(deadline);
        }
        ensure!(
            admission.deadline_ms > now_ms,
            "context recovery deadline exceeded before admission"
        );
        let snapshot = OperationSnapshot {
            id: uuid::Uuid::new_v4().to_string(),
            owner: prepared.owner,
            expected_checkpoint: prepared.expected_checkpoint,
            projection_version: prepared.projection_version,
            source_epochs: prepared.source_epochs,
            admission,
            // The bounded snapshot stores a descriptor, never another history.
            plan: CompactionPlan {
                mode: prepared.plan.mode,
                coverage_domain: prepared.plan.coverage_domain,
                compact: vec![],
                retain: vec![],
                coverage: vec![],
                fingerprint,
            },
        };
        store
            .compaction_admit_for_turn(workspace, thread, &snapshot, Some(&prepared.execution_turn))
            .await?
    };
    // The old request remains failed. A later admission may reuse its saved
    // portions under this request's unchanged budget; no background handoff or
    // automatic extension of an active operation is introduced here.
    if record.status == "failed" && record.outcome.as_deref() == Some("deadline") {
        store.compaction_reconcile_runner_state(&record.id).await?;
        let deadline = settings
            .admit(current, cli_override, now_ms)?
            .deadline_ms
            .min(prepared.operation_deadline_ms.unwrap_or(u64::MAX));
        if deadline > now_ms {
            store
                .compaction_resume_deadline(&record.id, &prepared.execution_turn, deadline)
                .await?;
            record = store
                .compaction_operation(&record.id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("operation missing"))?;
        }
    }
    let snapshot: OperationSnapshot = serde_json::from_str(&record.snapshot)?;
    if record.status != "running" || snapshot.admission.deadline_ms <= now_ms {
        return Ok(snapshot);
    }
    store
        .compaction_bind_execution_turn(&snapshot.id, &prepared.execution_turn)
        .await?;
    if let Some(projection) = &prepared.source_projection {
        let bound = store
            .compaction_bound_source_projection(&snapshot.id)
            .await?;
        if let Some(bound) = &bound {
            ensure!(
                bound.format == projection.format
                    && bound.messages == projection.messages
                    && bound.identity_sha256 == projection.identity_sha256,
                "resumed operation source projection changed"
            );
        }
        store
            .compaction_bind_source_projection(&snapshot.id, bound.as_ref().unwrap_or(projection))
            .await?;
    }
    store
        .compaction_prepare_runner(
            &snapshot.id,
            &budget,
            selected,
            prepared.manifest.len() as u64 - selected,
        )
        .await?;
    if store
        .compaction_runner_plan(&snapshot.id)
        .await?
        .is_some_and(|p| p.ready)
    {
        return Ok(snapshot);
    }
    // Split on both bounds. All transformation and serialization occurs after
    // the previous batch released its writer reservation.
    let mut start = 0;
    while start < prepared.manifest.len() {
        let mut end = start;
        let mut bytes = 0;
        while let Some(entry) = prepared.manifest.get(end) {
            let size = entry.thread_id.len()
                + entry.source.scope.len()
                + entry.source.id.len()
                + entry.source.version.len();
            ensure!(size <= SOURCE_PAGE_BYTES, "oversized manifest reference");
            if end - start >= SOURCE_PAGE_ROWS as usize || bytes + size > SOURCE_PAGE_BYTES {
                break;
            }
            bytes += size;
            end += 1;
        }
        store
            .compaction_append_manifest(&snapshot.id, &prepared.manifest[start..end])
            .await?;
        start = end;
    }
    if !store
        .compaction_manifest_sources_current(&snapshot.id)
        .await?
    {
        store
            .compaction_finish(&snapshot.id, "failed", "invalid_source_manifest")
            .await?;
        anyhow::bail!(
            "compaction source revisions or accepted imports do not match the admitted manifest"
        );
    }
    let initial = RunnerState::new(
        snapshot.admission.deadline_ms,
        &budget,
        prepared.target_tokens,
        prepared.summary_basis,
    )?;
    ensure!(
        initial.source_text_projection_version
            == pioneer_protocol::HISTORICAL_COMMAND_LLM_PROJECTION_VERSION,
        "runner and historical command projections disagree"
    );
    store
        .compaction_activate_runner(&snapshot.id, &initial)
        .await?;
    Ok(snapshot)
}
