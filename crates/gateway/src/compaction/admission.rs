//! Admission/resume contains metadata only; every database batch is bounded.
use super::*;
use pioneer_compaction::{CompactionPlan, CompactionSettings, ModelSelection, effective_selection};
use pioneer_crud::compaction::{ManifestEntry, SOURCE_PAGE_BYTES};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

#[derive(Clone)]
pub(crate) struct PreparedOperation {
    pub owner: String,
    pub execution_turn: String,
    pub source_projection: Option<pioneer_compaction::frozen::FrozenHistoryRef>,
    pub expected_checkpoint: Option<String>,
    /// A stale published head still participates in CAS, but its summary must
    /// not contaminate a rebuild from current canonical sources.
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
    prepared: PreparedOperation,
    now_ms: u64,
) -> Result<OperationSnapshot> {
    let store = store.with_maintenance_access();
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
    let record = if let Some(record) = existing {
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
    store
        .compaction_activate_runner(&snapshot.id, &initial)
        .await?;
    Ok(snapshot)
}
