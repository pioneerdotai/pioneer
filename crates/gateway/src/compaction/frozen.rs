//! Reference-only TaskRun context snapshots. Serialization never saves message
//! bodies; restoration verifies each referenced revision and its rendered hash.
use super::*;
use pioneer_agent::compaction::composition::ScopedHistorySource;
use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
use pioneer_crud::compaction::PreparedFrozenImport;
use pioneer_provider::{
    CanonicalProviderRoundEnvelope, ChatMessage, MessageProvenance, MessageSourceRef,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
static WORKSPACE_RESTORE_CALLS: std::sync::LazyLock<
    std::sync::Mutex<
        std::collections::HashMap<String, std::sync::Weak<std::sync::atomic::AtomicUsize>>,
    >,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
pub(crate) struct WorkspaceRestoreObserver {
    workspace: String,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl WorkspaceRestoreObserver {
    pub(crate) fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
impl Drop for WorkspaceRestoreObserver {
    fn drop(&mut self) {
        WORKSPACE_RESTORE_CALLS
            .lock()
            .unwrap()
            .remove(&self.workspace);
    }
}

#[cfg(test)]
pub(crate) fn observe_workspace_restores(workspace: &str) -> WorkspaceRestoreObserver {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let previous = WORKSPACE_RESTORE_CALLS
        .lock()
        .unwrap()
        .insert(workspace.to_owned(), std::sync::Arc::downgrade(&calls));
    assert!(
        previous.is_none(),
        "workspace restore observer already installed"
    );
    WorkspaceRestoreObserver {
        workspace: workspace.to_owned(),
        calls,
    }
}

/// Extend trusted execution scope by the TaskRun's durably accepted parent
/// basis. This does not change hook routing or admit arbitrary sibling history.
pub(crate) async fn execution_history_scopes(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    turn: &str,
    conversation_thread: Option<&str>,
) -> Result<BTreeSet<String>> {
    let mut allowed = BTreeSet::from([thread.to_owned()]);
    allowed.extend(conversation_thread.map(str::to_owned));
    if let Some(basis) = store
        .compaction_task_basis_snapshot(workspace, thread, turn)
        .await?
    {
        allowed.extend(
            accepted_history_scopes(store, workspace, &basis.parent_thread, &basis.history_json)
                .await?,
        );
    }
    Ok(allowed)
}

/// Only call after the TaskRun/runtime snapshot's destination identity has
/// been checked. The immutable, published manifest is the accepted grant; no
/// live ancestor traversal or grant from a model-provided source is involved.
pub(crate) async fn accepted_history_scopes(
    store: &CrudStore,
    workspace: &str,
    accepted_parent: &str,
    history_json: &str,
) -> Result<BTreeSet<String>> {
    let mut allowed = BTreeSet::from([accepted_parent.to_owned()]);
    if history_json.trim_start().starts_with('[') {
        return Ok(allowed);
    }
    let descriptor: FrozenHistoryRef = serde_json::from_str(history_json)?;
    ensure!(
        store
            .compaction_frozen_history_owner(workspace, &descriptor)
            .await?
            .as_deref()
            == Some(accepted_parent),
        "accepted Task manifest owner does not match its destination"
    );
    let mut ordinal = 0_u64;
    let mut digest = Sha256::new();
    while ordinal < descriptor.messages {
        let page = store
            .compaction_frozen_history_page(
                workspace,
                accepted_parent,
                &descriptor.manifest_id,
                ordinal,
            )
            .await?;
        ensure!(
            !page.is_empty(),
            "accepted Task manifest lost its reference page"
        );
        for reference in page {
            reference.validate()?;
            digest_entry(&mut digest, &reference)?;
            for reference in &reference.sources {
                if reference.scope.starts_with("checkpoint:") {
                    allowed.extend(
                        super::coverage::checkpoint_scopes(&store, workspace, reference).await?,
                    );
                }
            }
            allowed.insert(reference.source_thread);
            allowed.extend(reference.context_thread);
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("accepted manifest ordinal overflow"))?;
            ensure!(
                ordinal <= descriptor.messages,
                "accepted manifest count mismatch"
            );
        }
    }
    ensure!(
        hex::encode(digest.finalize()) == descriptor.identity_sha256,
        "accepted Task manifest digest mismatch"
    );
    Ok(allowed)
}

/// Hydrate only the explicit own imports admitted with this Task basis. The
/// caller binds the new execution ID through Task admission/lineage; model or
/// client references cannot select it. H retains its original context owner.
pub(crate) async fn hydrate_accepted_own(
    store: &CrudStore,
    workspace: &str,
    parent: &str,
    history_json: &str,
    execution_thread: &str,
    messages: &mut [ChatMessage],
) -> Result<()> {
    if history_json.trim_start().starts_with('[') {
        return Ok(());
    }
    ensure!(
        !execution_thread.is_empty(),
        "missing accepted execution context"
    );
    let descriptor: FrozenHistoryRef = serde_json::from_str(history_json)?;
    let accepted = read_accepted_imports(
        store,
        workspace,
        parent,
        &descriptor,
        messages,
        Some(execution_thread),
    )
    .await?;
    for index in accepted.into_keys() {
        let origin = messages[index].provenance.as_mut().unwrap();
        origin.context_thread = Some(execution_thread.into());
        origin.inherited = false;
    }
    Ok(())
}

async fn read_accepted_imports(
    store: &CrudStore,
    workspace: &str,
    parent: &str,
    descriptor: &FrozenHistoryRef,
    messages: &[ChatMessage],
    execution_thread: Option<&str>,
) -> Result<BTreeMap<usize, BTreeSet<SourceRef>>> {
    ensure!(
        store
            .compaction_frozen_history_owner(workspace, descriptor)
            .await?
            .as_deref()
            == Some(parent)
            && messages.len() as u64 == descriptor.messages,
        "accepted Task basis identity mismatch"
    );
    let (count, expected) = store
        .compaction_frozen_import_state(workspace, parent, &descriptor.manifest_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("accepted Task import metadata is incomplete"))?;
    let mut digest = Sha256::new();
    let mut ordinal = 0;
    let mut accepted = BTreeMap::<usize, BTreeSet<SourceRef>>::new();
    while ordinal < count {
        let page = store
            .compaction_frozen_import_page(workspace, parent, &descriptor.manifest_id, ordinal)
            .await?;
        ensure!(!page.is_empty(), "accepted Task import metadata has a gap");
        for record in page {
            let bytes = serde_json::to_vec(&record)?;
            digest.update((bytes.len() as u64).to_le_bytes());
            digest.update(bytes);
            let index = usize::try_from(record.message_ordinal)?;
            let origin = messages
                .get(index)
                .and_then(|message| message.provenance.as_ref())
                .ok_or_else(|| anyhow::anyhow!("accepted own import has no canonical message"))?;
            ensure!(
                origin.thread_id == record.source_thread
                    && origin
                        .sources
                        .iter()
                        .any(|reference| source(reference) == record.source)
                    && !origin.inherited
                    && origin.complete
                    && !origin.protected_input
                    && matches!(origin.context_thread.as_deref().unwrap_or(&origin.thread_id), owner if owner == parent || Some(owner) == execution_thread),
                "accepted own import no longer matches its frozen message"
            );
            accepted.entry(index).or_default().insert(record.source);
            ordinal += 1;
            ensure!(ordinal <= count, "accepted Task import count mismatch");
        }
    }
    ensure!(
        hex::encode(digest.finalize()) == expected,
        "accepted Task import digest mismatch"
    );
    for (index, sources) in &accepted {
        ensure!(
            messages[*index]
                .provenance
                .as_ref()
                .unwrap()
                .sources
                .iter()
                .all(|reference| sources.contains(&source(reference))),
            "mixed H and accepted own input requires a compatible projection"
        );
    }
    Ok(accepted)
}

fn wire_digest(message: &ChatMessage) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(message)?)))
}
fn source(reference: &MessageSourceRef) -> SourceRef {
    SourceRef {
        scope: reference.scope.clone(),
        id: reference.id.clone(),
        version: reference.version.clone(),
    }
}
fn digest_entry(digest: &mut Sha256, reference: &FrozenMessageRef) -> Result<()> {
    let bytes = serde_json::to_vec(reference)?;
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
    Ok(())
}
fn authorize(
    workspace: &str,
    allowed: &BTreeSet<String>,
    origin: &MessageProvenance,
) -> Result<()> {
    ensure!(
        origin.workspace_id == workspace
            && allowed.contains(&origin.thread_id)
            && origin
                .context_thread
                .as_ref()
                .is_none_or(|owner| allowed.contains(owner)),
        "frozen history source is outside the accepted context"
    );
    Ok(())
}

/// Capture a completed result-producing turn before publishing its candidate.
/// An accepted review or delayed delivery subsequently refers to this exact
/// work. Retries reuse the original manifest and never read newer child turns.
pub(crate) async fn capture_task_output(
    store: &CrudStore,
    workspace: &str,
    turn: &pioneer_protocol::TaskRunTurn,
) -> Result<pioneer_crud::compaction::TaskOutputSnapshot> {
    let store = store.with_maintenance_access();
    if let Some(snapshot) = store.compaction_task_output(workspace, &turn.id).await? {
        ensure!(
            snapshot.task_id == turn.task_id
                && snapshot.run_id == turn.run_id
                && snapshot.source_thread == turn.thread_id
                && snapshot.source_turn == turn.turn_id,
            "Task output snapshot execution identity changed"
        );
        return Ok(snapshot);
    }
    let started = std::time::Instant::now();
    // A delivered output is the child's own work, not another copy of its
    // accepted input context. H remains in the immutable TaskRun conversation
    // snapshot and in the recipient's context; C composes H + own A + own B.
    // In particular, do not hydrate/revalidate all of H's imported sources or
    // project a compaction checkpoint on the terminal delivery path.
    super::history::prepare_history(&store, workspace, &turn.thread_id).await?;
    let epoch = store
        .compaction_projection_version(workspace, &turn.thread_id)
        .await?;
    let fence = store.compaction_history_read_fence().await?;
    let messages = super::history::load_task_output_history(
        &store,
        workspace,
        &turn.thread_id,
        &turn.turn_id,
        &fence,
    )
    .await?;
    let history = capture(
        &store,
        workspace,
        &turn.thread_id,
        &BTreeSet::from([turn.thread_id.clone()]),
        &messages,
    )
    .await?;
    ensure!(
        store
            .compaction_projection_version(workspace, &turn.thread_id)
            .await?
            == epoch,
        "child history changed while freezing its completed output"
    );
    let output = store
        .compaction_record_task_output(workspace, &turn.id, &history)
        .await?;
    tracing::info!(
        elapsed_ms = started.elapsed().as_millis() as u64,
        message_count = history.messages,
        "completed Task output captured"
    );
    Ok(output)
}

/// `basis_turn` identifies the accepted parent execution independently of the
/// Composer-only exclusion. Ordinary Tasks include completed creator rounds.
pub(crate) async fn capture_execution_basis_json(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    basis_turn: Option<&str>,
    excluded_turn: Option<&str>,
    policy: Option<&pioneer_protocol::TaskAgentContextPolicy>,
) -> Result<String> {
    let prepared = capture_execution_basis_prepared_with_outputs(
        store,
        workspace,
        thread,
        basis_turn,
        excluded_turn,
        policy,
        None,
    )
    .await?;
    Ok(serde_json::to_string(&prepared.descriptor)?)
}

#[cfg(test)]
pub(super) async fn capture_execution_basis_with_outputs(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    basis_turn: Option<&str>,
    excluded_turn: Option<&str>,
    policy: Option<&pioneer_protocol::TaskAgentContextPolicy>,
    outputs: Option<&super::delivered::AuthorizedOutputSet>,
) -> Result<String> {
    let prepared = capture_execution_basis_prepared_with_outputs(
        store,
        workspace,
        thread,
        basis_turn,
        excluded_turn,
        policy,
        outputs,
    )
    .await?;
    Ok(serde_json::to_string(&prepared.descriptor)?)
}

pub(crate) struct PreparedHistory {
    pub(crate) descriptor: FrozenHistoryRef,
    pub(crate) messages: Vec<ChatMessage>,
}

pub(crate) async fn capture_execution_basis_prepared(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    basis_turn: Option<&str>,
    excluded_turn: Option<&str>,
    policy: Option<&pioneer_protocol::TaskAgentContextPolicy>,
) -> Result<PreparedHistory> {
    capture_execution_basis_prepared_with_outputs(
        store,
        workspace,
        thread,
        basis_turn,
        excluded_turn,
        policy,
        None,
    )
    .await
}

pub(super) async fn capture_execution_basis_prepared_with_outputs(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    basis_turn: Option<&str>,
    excluded_turn: Option<&str>,
    policy: Option<&pioneer_protocol::TaskAgentContextPolicy>,
    outputs: Option<&super::delivered::AuthorizedOutputSet>,
) -> Result<PreparedHistory> {
    if let Some(outputs) = outputs {
        ensure!(
            outputs.workspace == workspace && outputs.destination == thread,
            "accepted outputs belong to another context"
        );
    }
    let omits_history = policy.is_some_and(|policy| {
        matches!(
            policy.mode,
            pioneer_protocol::TaskAgentContextMode::Empty
                | pioneer_protocol::TaskAgentContextMode::Custom
        ) || (policy.mode == pioneer_protocol::TaskAgentContextMode::SummaryOnly
            && !policy.include_parent_summary)
    });
    if !omits_history && outputs.is_none() {
        super::history::prepare_history(&store, workspace, thread).await?;
    }
    let epoch = match outputs {
        Some(outputs) => *outputs
            .source_epochs
            .get(thread)
            .ok_or_else(|| anyhow::anyhow!("accepted outputs lost their destination epoch"))?,
        None => {
            store
                .compaction_projection_version(workspace, thread)
                .await?
        }
    };
    let fence = match outputs {
        Some(outputs) => outputs.fence.clone(),
        None => store.compaction_history_read_fence().await?,
    };
    let mut messages = if omits_history {
        Vec::new()
    } else {
        super::history::load_task_line_history(&store, workspace, thread, excluded_turn, &fence)
            .await?
    };
    let mut allowed = BTreeSet::from([thread.to_owned()]);
    let mut epochs = outputs
        .map(|outputs| outputs.source_epochs.clone())
        .unwrap_or_else(|| BTreeMap::from([(thread.to_owned(), epoch)]));
    let mut accepted_turn = basis_turn.map(str::to_owned);
    let mut imports = BTreeMap::new();
    let basis = if omits_history {
        None
    } else {
        let exact = if let Some(turn) = basis_turn {
            store
                .compaction_task_basis_snapshot(workspace, thread, turn)
                .await?
        } else {
            None
        };
        if exact.is_some() {
            exact
        } else if let Some(turn) = store
            .compaction_latest_task_basis_turn(workspace, thread, &fence)
            .await?
        {
            accepted_turn = Some(turn.clone());
            store
                .compaction_task_basis_snapshot(workspace, thread, &turn)
                .await?
        } else {
            None
        }
    };
    if let Some(basis) = basis {
        allowed.extend(
            accepted_history_scopes(&store, workspace, &basis.parent_thread, &basis.history_json)
                .await?,
        );
        for source_thread in &allowed {
            if !epochs.contains_key(source_thread) {
                epochs.insert(
                    source_thread.clone(),
                    store
                        .compaction_projection_version(workspace, source_thread)
                        .await?,
                );
            }
        }
        let mut inherited = crate::turn_runtime_snapshot::restore_history_json(
            &store,
            workspace,
            &allowed,
            &basis.history_json,
        )
        .await?;
        if basis.history_json.trim_start().starts_with('[') {
            let reference = store
                .compaction_legacy_task_basis_source(workspace, &basis.parent_thread, &basis.run_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("accepted legacy Task basis disappeared"))?;
            let body = super::history::reference_payload(
                &store,
                workspace,
                &basis.parent_thread,
                &reference,
            )
            .await?;
            ensure!(
                body == basis.history_json,
                "accepted legacy Task basis changed during capture"
            );
            // Retain the exact accepted messages as one opaque inherited unit.
            // No original-turn boundaries are inferred from the legacy array.
            for message in &mut inherited {
                message.provenance = Some(MessageProvenance {
                    logical_turn_id: None,
                    workspace_id: workspace.into(),
                    thread_id: basis.parent_thread.clone(),
                    context_thread: None,
                    unit_id: format!("legacy-task-basis:{}", basis.run_id),
                    sources: vec![MessageSourceRef {
                        scope: reference.scope.clone(),
                        id: reference.id.clone(),
                        version: reference.version.clone(),
                    }],
                    complete: true,
                    protected_input: false,
                    inherited: true,
                });
            }
        }
        hydrate_accepted_own(
            &store,
            workspace,
            &basis.parent_thread,
            &basis.history_json,
            thread,
            &mut inherited,
        )
        .await?;
        // Hydration promotes only explicitly accepted own imports. Preserve
        // that evidence when recapturing the child; provenance alone is not a grant.
        if !basis.history_json.trim_start().starts_with('[') {
            let descriptor: FrozenHistoryRef = serde_json::from_str(&basis.history_json)?;
            let (count, _) = store
                .compaction_frozen_import_state(
                    workspace,
                    &basis.parent_thread,
                    &descriptor.manifest_id,
                )
                .await?
                .ok_or_else(|| anyhow::anyhow!("accepted import state disappeared"))?;
            let turn = accepted_turn
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("accepted execution turn missing"))?;
            for ordinal in 0..count {
                let prepared = store
                    .compaction_prepare_accepted_import(workspace, thread, turn, ordinal)
                    .await?;
                imports.insert(
                    ScopedHistorySource {
                        thread: prepared.source_thread().into(),
                        source: prepared.source().clone(),
                    },
                    prepared,
                );
            }
        }
        messages = compose_frozen_basis(&store, workspace, thread, &allowed, &inherited, &messages)
            .await?;
    }
    let mut own_outputs = BTreeMap::<ScopedHistorySource, (usize, u64)>::new();
    if !omits_history && let Some(outputs) = outputs {
        for (branch_index, branch) in outputs.branches.iter().enumerate() {
            ensure!(
                store
                    .compaction_reference_thread(workspace, &branch.acknowledgement)
                    .await?
                    .as_deref()
                    == Some(thread),
                "accepted delivery acknowledgement changed"
            );
            allowed.extend(branch.source_threads.iter().cloned());
            let mut imported =
                restore(&store, workspace, &allowed, &branch.snapshot.output.history).await?;
            let layout = pioneer_agent::compaction::history::NativeHistoryLayout::from_messages(
                workspace,
                &branch.snapshot.output.source_thread,
                &imported,
                &vec![0; imported.len()],
            )?;
            for (unit, indexes) in layout.units.iter().zip(&layout.message_indexes) {
                if unit.role != pioneer_compaction::SourceRole::Own
                    || !unit.complete
                    || unit.protected_input
                {
                    continue;
                }
                for index in indexes {
                    let origin = imported[*index]
                        .provenance
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("output origin is missing"))?;
                    for reference in &origin.sources {
                        let reference = source(reference);
                        own_outputs
                            .entry(ScopedHistorySource {
                                thread: origin.thread_id.clone(),
                                source: reference.clone(),
                            })
                            .or_insert((branch_index, *index as u64));
                        if reference.scope.starts_with("checkpoint:") {
                            for leaf in super::coverage::checkpoint_leaves(
                                &store, workspace, &allowed, &reference,
                            )
                            .await?
                            {
                                own_outputs
                                    .entry(leaf)
                                    .or_insert((branch_index, *index as u64));
                            }
                        }
                    }
                }
            }
            let logical = store
                .compaction_task_delivery_command(workspace, thread, &branch.acknowledgement)
                .await?;
            for message in &mut imported {
                let origin = message.provenance.as_mut().ok_or_else(|| {
                    anyhow::anyhow!("accepted Task output has no source identity")
                })?;
                origin.context_thread = Some(thread.into());
                if !origin.inherited && logical.is_some() {
                    origin.logical_turn_id = logical.clone();
                }
            }
            // The delivered text is the transport copy of this exact output.
            // Denied branches never reach here and retain that disclosed text.
            messages = remove_delivered_projection(
                &store,
                workspace,
                thread,
                &allowed,
                messages,
                &branch.acknowledgements,
            )
            .await?;
            messages =
                compose_frozen_basis(&store, workspace, thread, &allowed, &messages, &imported)
                    .await?;
        }
    }
    let owner = super::native::native_owner(workspace, thread);
    if policy.is_none_or(|policy| {
        policy.include_parent_summary
            && !matches!(
                policy.mode,
                pioneer_protocol::TaskAgentContextMode::Empty
                    | pioneer_protocol::TaskAgentContextMode::Custom
            )
    }) && let Some(head) = store.compaction_head(&owner).await?
    {
        super::checkpoint::project_compatible_checkpoint(
            &store,
            workspace,
            thread,
            &owner,
            &head,
            &allowed,
            &mut messages,
        )
        .await?;
    }
    if let Some(policy) = policy {
        select_task_history(&mut messages, policy)?;
    }
    if let Some(outputs) = outputs {
        for message in &messages {
            let origin = message
                .provenance
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("accepted history lost its source origin"))?;
            if origin.inherited {
                continue;
            }
            for reference in &origin.sources {
                let key = ScopedHistorySource {
                    thread: origin.thread_id.clone(),
                    source: source(reference),
                };
                if let Some((branch_index, ordinal)) = own_outputs.get(&key) {
                    let branch = &outputs.branches[*branch_index];
                    let prepared = store
                        .compaction_prepare_frozen_import(
                            workspace,
                            thread,
                            &branch.snapshot.delivery_id,
                            &branch.acknowledgement,
                            *ordinal,
                            &key.thread,
                            &key.source,
                        )
                        .await?;
                    imports.insert(key, prepared);
                }
            }
        }
    }
    let prepared =
        capture_with_imports_prepared(&store, workspace, thread, &allowed, &messages, &imports)
            .await?;

    for (source_thread, expected) in epochs {
        ensure!(
            store
                .compaction_projection_version(workspace, &source_thread)
                .await?
                == expected,
            "parent history changed while freezing the accepted context"
        );
    }
    Ok(prepared)
}

/// Remove only identified transport copies. If a checkpoint already covers a
/// copy, first reconstruct its exact originals; never subtract text from it.
pub(super) async fn remove_delivered_projection(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    allowed: &BTreeSet<String>,
    mut messages: Vec<ChatMessage>,
    acknowledgements: &[SourceRef],
) -> Result<Vec<ChatMessage>> {
    use pioneer_agent::compaction::composition::ScopedHistorySource;
    let copies = acknowledgements.iter().cloned().collect::<BTreeSet<_>>();
    let affected = copies
        .iter()
        .map(|source| ScopedHistorySource {
            thread: thread.into(),
            source: source.clone(),
        })
        .collect::<BTreeSet<_>>();
    let mut checkpoints = BTreeMap::new();
    for message in &messages {
        let origin = message
            .provenance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("history has no source identity"))?;
        for reference in &origin.sources {
            if reference.scope.starts_with("checkpoint:") {
                let reference = source(reference);
                let closure =
                    super::coverage::checkpoint_leaves(store, workspace, allowed, &reference)
                        .await?;
                if !closure.is_disjoint(&affected) {
                    checkpoints.insert(
                        ScopedHistorySource {
                            thread: origin.thread_id.clone(),
                            source: reference,
                        },
                        closure,
                    );
                }
            }
        }
    }
    if !checkpoints.is_empty() {
        messages = super::compatible::rematerialize_overlap(
            store,
            workspace,
            allowed,
            &messages,
            &checkpoints,
            &affected,
        )
        .await?;
    }
    for message in &messages {
        let origin = message
            .provenance
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("history has no source identity"))?;
        if origin
            .sources
            .iter()
            .any(|reference| copies.contains(&source(reference)))
        {
            ensure!(
                origin.thread_id == thread && origin.sources.len() == 1,
                "delivery copy spans another canonical source"
            );
        }
    }
    messages.retain(|message| {
        !message
            .provenance
            .as_ref()
            .unwrap()
            .sources
            .iter()
            .any(|reference| copies.contains(&source(reference)))
    });
    Ok(messages)
}

pub(super) async fn compose_frozen_basis(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    allowed: &BTreeSet<String>,
    inherited: &[ChatMessage],
    own: &[ChatMessage],
) -> Result<Vec<ChatMessage>> {
    use pioneer_agent::compaction::composition::{
        AcceptedContextBranch, ScopedHistorySource, compose_context,
    };
    let mut checkpoints = BTreeMap::new();
    for message in inherited.iter().chain(own) {
        let origin = message.provenance.as_ref().ok_or_else(|| {
            anyhow::anyhow!("accepted Task basis has no canonical source identity")
        })?;
        for reference in &origin.sources {
            if reference.scope.starts_with("checkpoint:") {
                let source = source(reference);
                let key = ScopedHistorySource {
                    thread: origin.thread_id.clone(),
                    source: source.clone(),
                };
                if !checkpoints.contains_key(&key) {
                    checkpoints.insert(
                        key,
                        super::coverage::checkpoint_leaves(store, workspace, allowed, &source)
                            .await?,
                    );
                }
            }
        }
    }
    let mut inherited = inherited.to_vec();
    let mut own = own.to_vec();
    loop {
        let result = compose_context(
            workspace,
            thread,
            &[
                AcceptedContextBranch {
                    thread,
                    messages: &inherited,
                    checkpoints: &checkpoints,
                },
                AcceptedContextBranch {
                    thread,
                    messages: &own,
                    checkpoints: &checkpoints,
                },
            ],
        );
        let Err(error) = result else {
            return result;
        };
        let Some(overlap) = error
            .downcast_ref::<pioneer_agent::compaction::composition::CompatibleProjectionRequired>(
        ) else {
            return Err(error);
        };
        let compatible_inherited = super::compatible::rematerialize_overlap(
            store,
            workspace,
            allowed,
            &inherited,
            &checkpoints,
            &overlap.affected,
        )
        .await?;
        let compatible_own = super::compatible::rematerialize_overlap(
            store,
            workspace,
            allowed,
            &own,
            &checkpoints,
            &overlap.affected,
        )
        .await?;
        ensure!(
            compatible_inherited != inherited || compatible_own != own,
            "canonical units cannot form a compatible exact projection"
        );
        inherited = compatible_inherited;
        own = compatible_own;
    }
}

pub(crate) fn default_task_context_policy() -> pioneer_protocol::TaskAgentContextPolicy {
    pioneer_protocol::TaskAgentContextPolicy {
        mode: pioneer_protocol::TaskAgentContextMode::LastNTurns,
        max_turns: Some(6),
        include_parent_summary: true,
        include_artifacts: false,
        custom_context: None,
    }
}

fn select_task_history(
    messages: &mut Vec<ChatMessage>,
    policy: &pioneer_protocol::TaskAgentContextPolicy,
) -> Result<()> {
    use pioneer_protocol::TaskAgentContextMode as Mode;
    if matches!(policy.mode, Mode::Empty | Mode::Custom) {
        messages.clear();
        return Ok(());
    }
    let mut keys = Vec::with_capacity(messages.len());
    let mut last_occurrence = BTreeMap::new();
    for (index, message) in messages.iter().enumerate() {
        let origin = message.provenance.as_ref().ok_or_else(|| {
            anyhow::anyhow!("Task context selection requires canonical source identities")
        })?;
        ensure!(
            !origin.sources.is_empty(),
            "Task context message has no canonical sources"
        );
        let summary = origin
            .sources
            .iter()
            .all(|source| source.scope.starts_with("checkpoint:"));
        if summary {
            keys.push(None);
            continue;
        }
        let mut turns = origin
            .sources
            .iter()
            .map(|source| {
                source
                    .scope
                    .split_once(':')
                    .map(|(_, turn)| turn.to_owned())
                    .ok_or_else(|| anyhow::anyhow!("Task source has no canonical turn scope"))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        ensure!(
            turns.len() == 1,
            "Task message spans unrelated canonical turns"
        );
        let physical = turns.pop_first().expect("one source turn");
        let key = origin.logical_turn_id.clone().unwrap_or(physical);
        last_occurrence.insert(key.clone(), index);
        keys.push(Some(key));
    }
    let max_turns = match policy.mode {
        Mode::SummaryOnly => 0,
        Mode::InheritParent => policy.max_turns.unwrap_or(12).max(1) as usize,
        Mode::LastNTurns => policy.max_turns.unwrap_or(6).max(1) as usize,
        Mode::Empty | Mode::Custom => unreachable!(),
    };
    let mut turns = last_occurrence.into_iter().collect::<Vec<_>>();
    turns.sort_by_key(|(_, index)| *index);
    let selected = turns
        .into_iter()
        .rev()
        .take(max_turns)
        .map(|(turn, _)| turn)
        .collect::<BTreeSet<_>>();
    let mut index = 0;
    messages.retain(|_| {
        let keep = match &keys[index] {
            None => policy.include_parent_summary,
            Some(turn) => selected.contains(turn),
        };
        index += 1;
        keep
    });
    Ok(())
}

pub(crate) async fn capture(
    store: &CrudStore,
    workspace: &str,
    owner_thread: &str,
    allowed_threads: &BTreeSet<String>,
    messages: &[ChatMessage],
) -> Result<FrozenHistoryRef> {
    Ok(capture_with_imports_prepared(
        store,
        workspace,
        owner_thread,
        allowed_threads,
        messages,
        &BTreeMap::new(),
    )
    .await?
    .descriptor)
}

async fn capture_with_imports_prepared(
    store: &CrudStore,
    workspace: &str,
    owner_thread: &str,
    allowed_threads: &BTreeSet<String>,
    messages: &[ChatMessage],
    imports: &BTreeMap<ScopedHistorySource, PreparedFrozenImport>,
) -> Result<PreparedHistory> {
    ensure!(
        allowed_threads.contains(owner_thread),
        "frozen history owner is not authorized"
    );
    for message in messages {
        let origin = message.provenance.as_ref().ok_or_else(|| {
            anyhow::anyhow!("new frozen history requires canonical source identities")
        })?;
        authorize(workspace, allowed_threads, origin)?;
    }
    let mut references = Vec::with_capacity(messages.len());
    let mut digest = Sha256::new();
    for message in messages {
        let origin = message.provenance.as_ref().ok_or_else(|| {
            anyhow::anyhow!("new frozen history requires canonical source identities")
        })?;
        let mut reference = FrozenMessageRef {
            logical_turn_id: origin.logical_turn_id.clone(),
            source_thread: origin.thread_id.clone(),
            context_thread: origin.context_thread.clone(),
            unit_id: origin.unit_id.clone(),
            sources: origin.sources.iter().map(source).collect(),
            inherited: origin.inherited,
            complete: origin.complete,
            protected_input: origin.protected_input,
            wire_sha256: wire_digest(message)?,
            replay_source: None,
            tool_call_id: message.tool_call_id.clone(),
            tool_name: message.name.clone(),
        };
        reference.validate()?;
        if let [full_source] = reference.sources.as_slice()
            && let Some(turn) = full_source.scope.strip_prefix("item:")
        {
            super::history::prepare_references(
                store,
                workspace,
                &reference.source_thread,
                std::slice::from_ref(full_source),
            )
            .await?;
            let payload = super::history::reference_payload(
                &store,
                workspace,
                &reference.source_thread,
                full_source,
            )
            .await?;
            let item: pioneer_protocol::TurnItem = serde_json::from_str(&payload)?;
            reference.replay_source = store
                .compaction_context_reference_for_item(
                    workspace,
                    &reference.source_thread,
                    turn,
                    item.item_id(),
                    "tool_result_v2",
                )
                .await?;
        }
        digest_entry(&mut digest, &reference)?;
        references.push(reference);
    }
    let mut verified_messages = Vec::with_capacity(messages.len());
    for page in references.chunks(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize) {
        verified_messages
            .extend(restore_entries_page(store, workspace, allowed_threads, page).await?);
    }
    let mut accepted = Vec::new();
    for (ordinal, reference) in references.iter().enumerate() {
        for source in &reference.sources {
            if let Some(prepared) = imports.get(&ScopedHistorySource {
                thread: reference.source_thread.clone(),
                source: source.clone(),
            }) {
                accepted.push((ordinal as u64, prepared.clone()));
            }
        }
    }
    let import_digest = pioneer_crud::compaction::frozen_import_identity(&accepted)?;
    let identity_sha256 = hex::encode(digest.finalize());
    let capture_key = serde_json::to_vec(&(
        1_u32,
        workspace,
        owner_thread,
        &identity_sha256,
        references.len(),
        &import_digest,
        accepted.len(),
    ))?;
    let descriptor = FrozenHistoryRef {
        format: 1,
        manifest_id: format!("fh_{}", hex::encode(Sha256::digest(&capture_key))),
        messages: references.len() as u64,
        identity_sha256,
    };
    if let Some(existing) = store
        .compaction_equivalent_frozen_history(
            workspace,
            owner_thread,
            &descriptor,
            accepted.len() as u64,
            &import_digest,
        )
        .await?
    {
        if existing == descriptor {
            return Ok(PreparedHistory {
                descriptor: existing,
                messages: verified_messages,
            });
        }
        let messages = restore(store, workspace, allowed_threads, &existing).await?;
        return Ok(PreparedHistory {
            descriptor: existing,
            messages,
        });
    }
    store
        .compaction_begin_frozen_history_with_imports(
            workspace,
            owner_thread,
            &descriptor,
            accepted.len() as u64,
            &import_digest,
        )
        .await?;
    let (message_prefix, import_prefix) = store
        .compaction_share_frozen_prefix(
            workspace,
            owner_thread,
            &descriptor.manifest_id,
            &references,
            &accepted,
        )
        .await?;
    let mut start = usize::try_from(message_prefix)?;
    while start < references.len() {
        let mut end = start;
        let mut bytes = 0;
        while end < references.len()
            && end - start < pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize
        {
            let size = serde_json::to_vec(&references[end])?.len();
            ensure!(
                size <= pioneer_crud::compaction::SOURCE_PAGE_BYTES,
                "single frozen message reference exceeds storage quantum"
            );
            if bytes + size > pioneer_crud::compaction::SOURCE_PAGE_BYTES {
                break;
            }
            bytes += size;
            end += 1;
        }
        store
            .compaction_append_frozen_history(
                workspace,
                owner_thread,
                &descriptor.manifest_id,
                start as u64,
                &references[start..end],
            )
            .await?;
        start = end;
    }
    let mut start = usize::try_from(import_prefix)?;
    while start < accepted.len() {
        let mut end = start;
        let mut bytes = 0;
        while end < accepted.len()
            && end - start < pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize
        {
            let (ordinal, prepared) = &accepted[end];
            let size = prepared.estimated_write_bytes(&references[*ordinal as usize])?;
            ensure!(
                size <= pioneer_crud::compaction::FROZEN_IMPORT_PAGE_BYTES,
                "single import proof exceeds its metadata quantum"
            );
            if bytes + size > pioneer_crud::compaction::FROZEN_IMPORT_PAGE_BYTES {
                break;
            }
            bytes += size;
            end += 1;
        }
        store
            .compaction_append_frozen_imports(
                workspace,
                owner_thread,
                &descriptor.manifest_id,
                start as u64,
                &accepted[start..end],
            )
            .await?;
        start = end;
    }
    ensure!(
        store
            .compaction_finish_frozen_history(workspace, owner_thread, &descriptor)
            .await?,
        "frozen history publication failed"
    );
    Ok(PreparedHistory {
        descriptor,
        messages: verified_messages,
    })
}

pub(crate) async fn restore(
    store: &CrudStore,
    workspace: &str,
    allowed_threads: &BTreeSet<String>,
    descriptor: &FrozenHistoryRef,
) -> Result<Vec<ChatMessage>> {
    #[cfg(test)]
    if let Some(calls) = WORKSPACE_RESTORE_CALLS
        .lock()
        .unwrap()
        .get(workspace)
        .and_then(std::sync::Weak::upgrade)
    {
        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    let owner = store
        .compaction_frozen_history_owner(workspace, descriptor)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen history manifest is unavailable or incomplete"))?;
    ensure!(
        allowed_threads.contains(&owner),
        "frozen history owner is outside the accepted context"
    );
    let mut result = Vec::new();
    let mut digest = Sha256::new();
    while (result.len() as u64) < descriptor.messages {
        let page = store
            .compaction_frozen_history_page(
                workspace,
                &owner,
                &descriptor.manifest_id,
                result.len() as u64,
            )
            .await?;
        ensure!(
            !page.is_empty(),
            "frozen history lost an immutable reference page"
        );
        for reference in &page {
            reference.validate()?;
            ensure!(
                allowed_threads.contains(&reference.source_thread)
                    && reference
                        .context_thread
                        .as_ref()
                        .is_none_or(|owner| allowed_threads.contains(owner)),
                "frozen history source is outside the accepted context"
            );
            digest_entry(&mut digest, &reference)?;
        }
        result.extend(restore_entries_page(store, workspace, allowed_threads, &page).await?);
    }
    ensure!(
        result.len() as u64 == descriptor.messages
            && hex::encode(digest.finalize()) == descriptor.identity_sha256,
        "frozen history manifest digest mismatch"
    );
    let _ = read_accepted_imports(&store, workspace, &owner, descriptor, &result, None).await?;
    Ok(result)
}

async fn restore_entry(
    store: &CrudStore,
    workspace: &str,
    allowed_threads: &BTreeSet<String>,
    reference: &FrozenMessageRef,
) -> Result<ChatMessage> {
    reference.validate()?;
    for source in &reference.sources {
        if source.scope.starts_with("checkpoint:") {
            super::coverage::checkpoint_leaves(store, workspace, allowed_threads, source).await?;
        }
    }
    if reference
        .sources
        .iter()
        .all(|source| source.scope.starts_with("input:"))
    {
        let mut inputs = Vec::with_capacity(reference.sources.len());
        let mut offset = 0usize;
        while offset < reference.sources.len() {
            let (consumed, payloads) = store
                .compaction_reference_payload_batch(
                    workspace,
                    &reference.source_thread,
                    &reference.sources[offset..],
                )
                .await?;
            ensure!(
                consumed > 0 && consumed == payloads.len(),
                "frozen input batch made no progress"
            );
            #[cfg(test)]
            let _raw_payloads = super::history::observe_payload_batch(&payloads);
            for payload in payloads {
                inputs.push(serde_json::from_str::<pioneer_protocol::UserInput>(
                    &payload,
                )?);
            }
            offset += consumed;
        }
        return finish_restored_entry(
            store,
            workspace,
            reference,
            vec![super::history::input_message(&inputs)?],
        )
        .await;
    }
    ensure!(
        reference.sources.len() == 1,
        "unsupported composite frozen projection"
    );
    let payload = super::history::reference_payload(
        store,
        workspace,
        &reference.source_thread,
        &reference.sources[0],
    )
    .await?;
    restore_entry_from_payloads(store, workspace, reference, std::slice::from_ref(&payload)).await
}

/// Restore a bounded manifest page. Consecutive single-source input/context
/// entries share the CRUD byte/row bounded read; each message is projected and
/// wire-checked only after that reader has been released.
async fn restore_entries_page(
    store: &CrudStore,
    workspace: &str,
    allowed_threads: &BTreeSet<String>,
    references: &[FrozenMessageRef],
) -> Result<Vec<ChatMessage>> {
    let mut result = Vec::with_capacity(references.len());
    let mut index = 0usize;
    while index < references.len() {
        let reference = &references[index];
        let batchable = reference.sources.len() == 1
            && reference.replay_source.is_none()
            && matches!(
                reference.sources[0]
                    .scope
                    .split_once(':')
                    .map(|(kind, _)| kind),
                Some("input" | "context")
            );
        if !batchable {
            let mut sources = reference.sources.clone();
            sources.extend(reference.replay_source.iter().cloned());
            super::history::prepare_references(
                store,
                workspace,
                &reference.source_thread,
                &sources,
            )
            .await?;
            result.push(restore_entry(store, workspace, allowed_threads, reference).await?);
            index += 1;
            continue;
        }
        let end = references[index..]
            .iter()
            .take(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize)
            .take_while(|next| {
                next.source_thread == reference.source_thread
                    && next.sources.len() == 1
                    && next.replay_source.is_none()
                    && matches!(
                        next.sources[0].scope.split_once(':').map(|(kind, _)| kind),
                        Some("input" | "context")
                    )
            })
            .count();
        let sources = references[index..index + end]
            .iter()
            .map(|entry| entry.sources[0].clone())
            .collect::<Vec<_>>();
        super::history::prepare_references(store, workspace, &reference.source_thread, &sources)
            .await?;
        let (consumed, payloads) = store
            .compaction_reference_payload_batch(workspace, &reference.source_thread, &sources)
            .await?;
        ensure!(
            consumed > 0 && consumed == payloads.len(),
            "frozen payload batch made no progress"
        );
        #[cfg(test)]
        let _raw_payloads = super::history::observe_payload_batch(&payloads);
        for (entry, payload) in references[index..index + consumed].iter().zip(payloads) {
            result.push(
                restore_entry_from_payloads(
                    store,
                    workspace,
                    entry,
                    std::slice::from_ref(&payload),
                )
                .await?,
            );
        }
        index += consumed;
    }
    Ok(result)
}

async fn restore_entry_from_payloads(
    store: &CrudStore,
    workspace: &str,
    reference: &FrozenMessageRef,
    payloads: &[String],
) -> Result<ChatMessage> {
    ensure!(
        payloads.len() == reference.sources.len(),
        "frozen payload count mismatch"
    );
    let mut candidates = Vec::new();
    if reference
        .sources
        .iter()
        .all(|source| source.scope.starts_with("input:"))
    {
        let inputs = payloads
            .iter()
            .map(|payload| serde_json::from_str(payload))
            .collect::<std::result::Result<Vec<pioneer_protocol::UserInput>, _>>()?;
        candidates.push(super::history::input_message(&inputs)?);
    } else {
        ensure!(
            reference.sources.len() == 1,
            "unsupported composite frozen projection"
        );
        let source = &reference.sources[0];
        let payload = &payloads[0];
        if source.scope.starts_with("event:") {
            let event: pioneer_crud::CanonicalTurnEventPayload = serde_json::from_str(payload)?;
            ensure!(
                event.workspace_id() == workspace && event.thread_id() == reference.source_thread,
                "frozen event scope mismatch"
            );
            // Previously frozen failed-event projections omitted terminal
            // status. Retain that exact wire form for existing descriptors;
            // newly captured history distinguishes Interrupted from Failed.
            if let pioneer_crud::CanonicalTurnEventPayload::TurnFailed(value) = &event {
                candidates.push(ChatMessage::user(format!(
                    "Historical turn failed: {:?}",
                    value.turn.error
                )));
            }
            if let Some(message) = super::history::event_message(event)? {
                candidates.push(message);
            }
        } else if source.scope.starts_with("task-basis:") {
            candidates.extend(serde_json::from_str::<Vec<ChatMessage>>(payload)?);
        } else if source.scope.starts_with("checkpoint:") {
            candidates.push(ChatMessage::user(format!(
                "Summary of completed work (historical data):\n{payload}"
            )));
        } else if source.scope.starts_with("context:") {
            if let Ok(envelope) = serde_json::from_str::<CanonicalProviderRoundEnvelope>(payload) {
                if envelope.termination == pioneer_provider::ProviderTermination::ProviderError {
                    candidates.push(super::history::provider_observation(payload)?);
                } else {
                    candidates.push(envelope.message);
                }
            } else if let Ok(view) = serde_json::from_str::<pioneer_tools::ToolResultView>(payload)
            {
                if let pioneer_tools::ToolResultView::Json {
                    value,
                    truncated: false,
                } = view
                {
                    let message: ChatMessage = serde_json::from_value(value)?;
                    candidates.push(message);
                }
            }
            candidates.push(ChatMessage::user(format!(
                "Legacy provider observation (available original):\n{payload}"
            )));
            candidates.push(ChatMessage::user(format!(
                "Legacy provider observation; outcome is not inferred:\n{payload}"
            )));
        } else if let Some(turn) = source.scope.strip_prefix("item:") {
            let item: pioneer_protocol::TurnItem = serde_json::from_str(payload)?;
            if let Some(replay) = &reference.replay_source {
                ensure!(
                    replay.scope == format!("context:{turn}"),
                    "frozen tool replay crosses turn scope"
                );
                let body = super::history::reference_payload(
                    store,
                    workspace,
                    &reference.source_thread,
                    replay,
                )
                .await?;
                let view: pioneer_tools::ToolResultView = serde_json::from_str(&body)?;
                if let pioneer_tools::ToolResultView::Json {
                    value,
                    truncated: false,
                } = view
                {
                    candidates.push(serde_json::from_value(value)?);
                }
            } else if let (Some(call), Some(name)) = (&reference.tool_call_id, &reference.tool_name)
                && let Some((current, message)) = super::retained_shell_outcome(
                    store,
                    workspace,
                    &reference.source_thread,
                    turn,
                    item.item_id(),
                    call,
                    name,
                )
                .await?
            {
                ensure!(&current == source, "frozen terminal tool revision changed");
                candidates.push(message);
            }
        }
    }
    finish_restored_entry(store, workspace, reference, candidates).await
}

async fn finish_restored_entry(
    store: &CrudStore,
    workspace: &str,
    reference: &FrozenMessageRef,
    mut candidates: Vec<ChatMessage>,
) -> Result<ChatMessage> {
    let replay_source = reference
        .replay_source
        .as_ref()
        .unwrap_or(&reference.sources[0]);
    if candidates
        .iter()
        .any(|message| message.role == pioneer_provider::Role::Tool)
        && let Some(item) = store
            .compaction_replay_item_id(workspace, &reference.source_thread, replay_source)
            .await?
    {
        let (_, turn) = replay_source
            .scope
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("invalid frozen replay scope"))?;
        let locator=serde_json::json!({"workspace_id":workspace,"thread_id":reference.source_thread,"turn_id":turn,"item_id":item}).to_string();
        let full = candidates.clone();
        for message in full
            .into_iter()
            .filter(|message| message.role == pioneer_provider::Role::Tool)
        {
            candidates.push(pioneer_agent::compaction::restored_tool_result_message(
                &message, &locator,
            )?);
        }
    }
    // The wire hash chooses an exact deterministic projection of a known source,
    // never a similar text or an inferred coverage boundary.
    let base = candidates.clone();
    for message in base {
        candidates.push(ChatMessage::user(format!("Interrupted canonical round; some tool outcomes are unknown. Historical observation, not a new call:\n{}",serde_json::to_string(&message)?)));
    }
    let mut message = candidates
        .into_iter()
        .find_map(|message| match wire_digest(&message) {
            Ok(hash) if hash == reference.wire_sha256 => Some(message),
            _ => None,
        })
        .ok_or_else(|| {
            anyhow::anyhow!("frozen source no longer renders the captured model message")
        })?;
    message.provenance = Some(MessageProvenance {
        logical_turn_id: reference.logical_turn_id.clone(),
        workspace_id: workspace.into(),
        thread_id: reference.source_thread.clone(),
        context_thread: reference.context_thread.clone(),
        unit_id: reference.unit_id.clone(),
        sources: reference
            .sources
            .iter()
            .map(|source| MessageSourceRef {
                scope: source.scope.clone(),
                id: source.id.clone(),
                version: source.version.clone(),
            })
            .collect(),
        complete: reference.complete,
        protected_input: reference.protected_input,
        inherited: reference.inherited,
    });
    Ok(message)
}

#[cfg(test)]
mod policy_tests {
    use super::*;

    #[test]
    fn compaction_last_turn_keeps_exact_task_command_with_its_later_outcome() {
        let mut history = Vec::new();
        for (turn, logical, text) in [
            ("command", None, "Task command"),
            ("unrelated", None, "other work"),
            ("delivery", Some("command"), "Task outcome"),
        ] {
            let mut message = ChatMessage::user(text);
            message.provenance = Some(MessageProvenance {
                logical_turn_id: logical.map(str::to_owned),
                workspace_id: "ws".into(),
                thread_id: "thread".into(),
                context_thread: None,
                unit_id: turn.into(),
                sources: vec![MessageSourceRef {
                    scope: format!("event:{turn}"),
                    id: turn.into(),
                    version: "event-revision:1".into(),
                }],
                complete: true,
                protected_input: false,
                inherited: false,
            });
            history.push(message);
        }
        select_task_history(
            &mut history,
            &pioneer_protocol::TaskAgentContextPolicy {
                max_turns: Some(1),
                ..default_task_context_policy()
            },
        )
        .unwrap();
        assert_eq!(
            history
                .iter()
                .map(|m| m.content.as_str())
                .collect::<Vec<_>>(),
            vec!["Task command", "Task outcome"]
        );
    }

    #[test]
    fn compaction_task_policy_counts_source_turns_and_keeps_whole_rounds() {
        let mut history = Vec::new();
        for turn in 0..14 {
            for ordinal in 0..3 {
                let mut message = ChatMessage::user(format!("turn {turn} part {ordinal}"));
                message.provenance = Some(MessageProvenance {
                    logical_turn_id: None,
                    workspace_id: "ws".into(),
                    thread_id: "parent".into(),
                    context_thread: None,
                    unit_id: format!("turn-{turn}:round"),
                    sources: vec![MessageSourceRef {
                        scope: format!("context:turn-{turn}"),
                        id: format!("source-{turn}-{ordinal}"),
                        version: "fixture-version".into(),
                    }],
                    inherited: false,
                    complete: true,
                    protected_input: false,
                });
                history.push(message);
            }
        }
        let policy = default_task_context_policy();
        let mut default_history = history.clone();
        select_task_history(&mut default_history, &policy).unwrap();
        assert_eq!(default_history, history[8 * 3..]);
        let mut inherited = history.clone();
        select_task_history(
            &mut inherited,
            &pioneer_protocol::TaskAgentContextPolicy {
                mode: pioneer_protocol::TaskAgentContextMode::InheritParent,
                max_turns: None,
                ..policy.clone()
            },
        )
        .unwrap();
        assert_eq!(inherited, history[2 * 3..]);
        let mut last_two = history.clone();
        select_task_history(
            &mut last_two,
            &pioneer_protocol::TaskAgentContextPolicy {
                max_turns: Some(2),
                ..policy
            },
        )
        .unwrap();
        assert_eq!(last_two, history[12 * 3..]);
    }
}
