//! Reference-only TaskRun context snapshots. Serialization never saves message
//! bodies; restoration verifies each referenced revision and its rendered hash.
use super::*;
use anyhow::Context;
use pioneer_agent::compaction::composition::ScopedHistorySource;
use pioneer_compaction::frozen::{FrozenHistoryRef, FrozenMessageRef};
use pioneer_crud::compaction::{DeliveryCheckpointImportSource, PreparedFrozenImport};
use pioneer_provider::{
    CanonicalProviderRoundEnvelope, ChatMessage, MessageProvenance, MessageSourceRef,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
static STORE_RESTORE_CALLS: std::sync::LazyLock<
    std::sync::Mutex<
        std::collections::HashMap<(usize, String), std::sync::Weak<StoreRestoreState>>,
    >,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
type ContinuitySourceLookupRegistry = std::sync::Mutex<
    std::collections::HashMap<(usize, String), std::sync::Weak<std::sync::atomic::AtomicUsize>>,
>;

#[cfg(test)]
static CONTINUITY_SOURCE_LOOKUPS: std::sync::LazyLock<ContinuitySourceLookupRegistry> =
    std::sync::LazyLock::new(Default::default);

#[cfg(test)]
pub(crate) struct ContinuitySourceLookupObserver {
    key: (usize, String),
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(test)]
impl ContinuitySourceLookupObserver {
    pub(crate) fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
impl Drop for ContinuitySourceLookupObserver {
    fn drop(&mut self) {
        CONTINUITY_SOURCE_LOOKUPS.lock().unwrap().remove(&self.key);
    }
}

#[cfg(test)]
pub(crate) fn observe_continuity_source_lookups(
    store: &CrudStore,
    workspace: &str,
) -> ContinuitySourceLookupObserver {
    let key = (
        store.database_connection().runtime_identity(),
        workspace.to_owned(),
    );
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let previous = CONTINUITY_SOURCE_LOOKUPS
        .lock()
        .unwrap()
        .insert(key.clone(), std::sync::Arc::downgrade(&calls));
    assert!(
        previous.is_none(),
        "continuity lookup observer already installed"
    );
    ContinuitySourceLookupObserver { key, calls }
}

#[cfg(test)]
#[derive(Default)]
struct StoreRestoreState {
    descriptors: std::sync::Mutex<Vec<FrozenHistoryRef>>,
}

#[cfg(test)]
pub(crate) struct StoreRestoreObserver {
    key: (usize, String),
    state: std::sync::Arc<StoreRestoreState>,
}

#[cfg(test)]
impl StoreRestoreObserver {
    pub(crate) fn calls(&self) -> usize {
        self.state.descriptors.lock().unwrap().len()
    }

    pub(crate) fn calls_for(&self, descriptor: &FrozenHistoryRef) -> usize {
        self.state
            .descriptors
            .lock()
            .unwrap()
            .iter()
            .filter(|restored| *restored == descriptor)
            .count()
    }
}

#[cfg(test)]
impl Drop for StoreRestoreObserver {
    fn drop(&mut self) {
        STORE_RESTORE_CALLS.lock().unwrap().remove(&self.key);
    }
}

#[cfg(test)]
pub(crate) fn observe_store_restores(store: &CrudStore, workspace: &str) -> StoreRestoreObserver {
    let key = (
        store.database_connection().runtime_identity(),
        workspace.to_owned(),
    );
    let state = std::sync::Arc::new(StoreRestoreState::default());
    let previous = STORE_RESTORE_CALLS
        .lock()
        .unwrap()
        .insert(key.clone(), std::sync::Arc::downgrade(&state));
    assert!(
        previous.is_none(),
        "store restore observer already installed"
    );
    StoreRestoreObserver { key, state }
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
    let basis = if let Some(exact) = store
        .compaction_task_basis_snapshot(workspace, thread, turn)
        .await?
    {
        Some(exact)
    } else {
        // Manual turns in an existing Task child are not TaskRun turns. Use
        // the same admitted child basis selection as canonical preparation;
        // never derive authority from the current parent transcript.
        let fence = store.compaction_history_read_fence().await?;
        if let Some(basis_turn) = store
            .compaction_latest_task_basis_turn(workspace, thread, &fence)
            .await?
        {
            store
                .compaction_task_basis_snapshot(workspace, thread, &basis_turn)
                .await?
        } else {
            None
        }
    };
    if let Some(basis) = basis {
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
    accepted_history_scopes_prepared(store, workspace, accepted_parent, history_json).await
}

async fn accepted_history_scopes_prepared(
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
    hydrate_accepted_own_view(
        store,
        workspace,
        parent,
        history_json,
        execution_thread,
        messages,
        AcceptedHydrationView::Execution,
    )
    .await
}

/// The insert-if-absent loser restores the accepted manifest literally. Its
/// message positions are manifest ordinals, including model-invisible entries.
pub(crate) async fn hydrate_accepted_own_literal(
    store: &CrudStore,
    workspace: &str,
    parent: &str,
    history_json: &str,
    execution_thread: &str,
    messages: &mut [ChatMessage],
) -> Result<()> {
    hydrate_accepted_own_view(
        store,
        workspace,
        parent,
        history_json,
        execution_thread,
        messages,
        AcceptedHydrationView::Literal,
    )
    .await
}

#[derive(Clone, Copy)]
enum AcceptedHydrationView {
    Literal,
    Execution,
}

async fn hydrate_accepted_own_view(
    store: &CrudStore,
    workspace: &str,
    parent: &str,
    history_json: &str,
    execution_thread: &str,
    messages: &mut [ChatMessage],
    view: AcceptedHydrationView,
) -> Result<()> {
    if history_json.trim_start().starts_with('[') {
        return Ok(());
    }
    ensure!(
        !execution_thread.is_empty(),
        "missing accepted execution context"
    );
    let descriptor: FrozenHistoryRef = serde_json::from_str(history_json)?;
    let (_, references) =
        frozen_manifest_references(store, workspace, Some(parent), &descriptor, None).await?;
    let mut checkpoint_graphs = super::coverage::CheckpointGraphResolver::default();
    let accepted = read_accepted_imports(
        store,
        workspace,
        parent,
        &descriptor,
        &references,
        Some(execution_thread),
        &mut checkpoint_graphs,
    )
    .await?;
    if accepted.is_empty() {
        return Ok(());
    }

    if matches!(view, AcceptedHydrationView::Literal) {
        ensure!(
            messages.len() == references.len(),
            "accepted Task basis identity mismatch"
        );
        return hydrate_visible_messages(
            messages,
            &references,
            &(0..references.len()).collect::<Vec<_>>(),
            &accepted,
            execution_thread,
        );
    }

    // Provenance is not a per-message key: legacy task-basis arrays assign
    // the same source/unit identity to every message. Reconstruct the visible
    // ordinal sequence once, preserving gaps left by today's typed policy,
    // then pair that sequence positionally with the already restored history.
    // This is linear, accepts duplicate identities, and never guesses which
    // duplicate an accepted import belongs to.
    let mut allowed = BTreeSet::from([parent.to_owned()]);
    for reference in &references {
        allowed.insert(reference.source_thread.clone());
        allowed.extend(reference.context_thread.clone());
    }
    let mut visible_ordinals = Vec::with_capacity(messages.len());
    let mut restore_state = FrozenExecutionRestoreState::from_references(
        store,
        workspace,
        &allowed,
        &references,
        &mut checkpoint_graphs,
    )
    .await?;
    for (page_index, page) in references
        .chunks(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize)
        .enumerate()
    {
        let restored = restore_execution_entries_page(
            store,
            workspace,
            &allowed,
            page,
            &mut checkpoint_graphs,
            &mut restore_state,
        )
        .await?;
        ensure!(
            restored.len() == page.len(),
            "frozen history count mismatch"
        );
        let page_start = page_index * pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize;
        visible_ordinals.extend(
            restored
                .into_iter()
                .enumerate()
                .filter_map(|(page_ordinal, entry)| entry.map(|_| page_start + page_ordinal)),
        );
    }
    ensure!(
        messages.len() == visible_ordinals.len(),
        "accepted Task basis identity mismatch"
    );
    hydrate_visible_messages(
        messages,
        &references,
        &visible_ordinals,
        &accepted,
        execution_thread,
    )
}

fn hydrate_visible_messages(
    messages: &mut [ChatMessage],
    references: &[FrozenMessageRef],
    visible_ordinals: &[usize],
    accepted: &BTreeMap<usize, AcceptedMessageImports>,
    execution_thread: &str,
) -> Result<()> {
    ensure!(
        messages.len() == visible_ordinals.len(),
        "accepted Task basis identity mismatch"
    );
    for (message, ordinal) in messages.iter_mut().zip(visible_ordinals.iter().copied()) {
        let origin = message
            .provenance
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("accepted Task basis message has no provenance"))?;
        ensure!(
            message_origin_identity(origin) == frozen_reference_identity(&references[ordinal]),
            "accepted Task basis identity mismatch"
        );
        if accepted.contains_key(&ordinal) {
            origin.context_thread = Some(execution_thread.into());
            origin.inherited = false;
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct FrozenProvenanceIdentity {
    logical_turn_id: Option<String>,
    source_thread: String,
    unit_id: String,
    sources: Vec<SourceRef>,
    complete: bool,
    protected_input: bool,
}

fn frozen_reference_identity(reference: &FrozenMessageRef) -> FrozenProvenanceIdentity {
    FrozenProvenanceIdentity {
        logical_turn_id: reference.logical_turn_id.clone(),
        source_thread: reference.source_thread.clone(),
        unit_id: reference.unit_id.clone(),
        sources: reference.sources.clone(),
        complete: reference.complete,
        protected_input: reference.protected_input,
    }
}

fn message_origin_identity(origin: &MessageProvenance) -> FrozenProvenanceIdentity {
    FrozenProvenanceIdentity {
        logical_turn_id: origin.logical_turn_id.clone(),
        source_thread: origin.thread_id.clone(),
        unit_id: origin.unit_id.clone(),
        sources: origin.sources.iter().map(source).collect(),
        complete: origin.complete,
        protected_input: origin.protected_input,
    }
}

async fn read_accepted_imports(
    store: &CrudStore,
    workspace: &str,
    parent: &str,
    descriptor: &FrozenHistoryRef,
    references: &[FrozenMessageRef],
    execution_thread: Option<&str>,
    checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
) -> Result<BTreeMap<usize, AcceptedMessageImports>> {
    ensure!(
        store
            .compaction_frozen_history_owner(workspace, descriptor)
            .await?
            .as_deref()
            == Some(parent)
            && references.len() as u64 == descriptor.messages,
        "accepted Task basis identity mismatch"
    );
    let (count, expected) = store
        .compaction_frozen_import_state(workspace, parent, &descriptor.manifest_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("accepted Task import metadata is incomplete"))?;
    let mut digest = Sha256::new();
    let mut ordinal = 0;
    let mut accepted = BTreeMap::<usize, AcceptedMessageImports>::new();
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
            let origin = references
                .get(index)
                .ok_or_else(|| anyhow::anyhow!("accepted own import has no canonical message"))?;
            let direct_source = origin.source_thread == record.source_thread
                && origin.sources.iter().any(|source| source == &record.source);
            let checkpoint_target = match origin.sources.as_slice() {
                [source] if source.scope.starts_with("checkpoint:") => Some(ScopedHistorySource {
                    thread: origin.source_thread.clone(),
                    source: source.clone(),
                }),
                _ => None,
            };
            ensure!(
                (direct_source || checkpoint_target.is_some())
                    && !origin.inherited
                    && origin.complete
                    && !origin.protected_input
                    && matches!(origin.context_thread.as_deref().unwrap_or(&origin.source_thread), owner if owner == parent || Some(owner) == execution_thread),
                "accepted own import no longer matches its frozen message"
            );
            let entry = accepted.entry(index).or_default();
            if let Some(target) = checkpoint_target {
                if let Some(existing) = &entry.checkpoint_target {
                    ensure!(
                        existing == &target,
                        "accepted checkpoint imports disagree on their target"
                    );
                } else {
                    entry.checkpoint_target = Some(target);
                }
            }
            entry.sources.insert(ScopedHistorySource {
                thread: record.source_thread,
                source: record.source,
            });
            entry.import_ordinals.push(ordinal);
            ordinal += 1;
            ensure!(ordinal <= count, "accepted Task import count mismatch");
        }
    }
    ensure!(
        hex::encode(digest.finalize()) == expected,
        "accepted Task import digest mismatch"
    );
    for (index, imports) in &accepted {
        let reference = &references[*index];
        let direct_sources = reference
            .sources
            .iter()
            .map(|source| ScopedHistorySource {
                thread: reference.source_thread.clone(),
                source: source.clone(),
            })
            .collect::<BTreeSet<_>>();
        if imports.sources == direct_sources {
            continue;
        }
        let [checkpoint] = reference.sources.as_slice() else {
            anyhow::bail!("mixed H and accepted own input requires a compatible projection");
        };
        ensure!(
            checkpoint.scope.starts_with("checkpoint:"),
            "accepted own import no longer matches its frozen message"
        );
        ensure!(
            checkpoint_graphs
                .authorized_by_historical_inputs(store, workspace, checkpoint, &imports.sources)
                .await?,
            "accepted checkpoint replacement exceeds its immutable grants"
        );
    }
    Ok(accepted)
}

#[derive(Default)]
struct AcceptedMessageImports {
    sources: BTreeSet<ScopedHistorySource>,
    import_ordinals: Vec<u64>,
    checkpoint_target: Option<ScopedHistorySource>,
}

fn wire_digest(message: &ChatMessage) -> Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(message)?)))
}

#[derive(Clone)]
struct FrozenModelCandidate {
    wire: ChatMessage,
    model: ChatMessage,
}

impl FrozenModelCandidate {
    fn exact(message: ChatMessage) -> Self {
        Self {
            wire: message.clone(),
            model: message,
        }
    }

    fn upgraded(wire: ChatMessage, model: ChatMessage) -> Self {
        Self { wire, model }
    }
}

fn verified_model_candidate(
    wire_sha256: &str,
    candidates: Vec<FrozenModelCandidate>,
) -> Result<FrozenModelCandidate> {
    candidates
        .into_iter()
        .find_map(|candidate| match wire_digest(&candidate.wire) {
            Ok(hash) if hash == wire_sha256 => Some(candidate),
            _ => None,
        })
        .ok_or_else(|| {
            anyhow::anyhow!("frozen source no longer renders the captured model message")
        })
}
#[cfg(test)]
fn verified_model_message(
    wire_sha256: &str,
    candidates: Vec<FrozenModelCandidate>,
) -> Result<ChatMessage> {
    Ok(verified_model_candidate(wire_sha256, candidates)?.model)
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
    // Preserve the caller's scope: live completion is request work, while
    // recovery callers already carry maintenance reads/critical writes.
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
    super::history::prepare_history(store, workspace, &turn.thread_id).await?;
    let epoch = store
        .compaction_projection_version(workspace, &turn.thread_id)
        .await?;
    let fence = store.compaction_history_read_fence().await?;
    let messages = super::history::load_task_output_history(
        store,
        workspace,
        &turn.thread_id,
        &turn.turn_id,
        &fence,
    )
    .await?;
    let history = capture(
        store,
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
    /// Scopes admitted while the messages and descriptor were prepared. This
    /// is request-owned context, not a permission token for another request.
    pub(crate) accepted_scopes: BTreeSet<String>,
    /// Projection epochs checked at the end of capture and retained as the
    /// accepted scope binding. Publication rechecks direct raw references;
    /// checkpoint-only history does not inherit old leaf epochs.
    pub(crate) source_epochs: BTreeMap<String, u64>,
    /// Actual native head captured before the accepted history fence. This is
    /// the publication CAS target even when projection must use an ancestor.
    pub(crate) expected_checkpoint: Option<String>,
    /// Published checkpoint compatible with this accepted history, possibly an
    /// ancestor of the captured head. `None` means no summary fits this exact
    /// boundary. This is the projection basis, not the publication CAS target.
    pub(crate) checkpoint: Option<String>,
    /// Historical coverage metadata cached only for this preparation. Reuse
    /// rechecks the published root and accepted scopes, never canonical leaves.
    pub(crate) checkpoint_graphs: super::coverage::CheckpointGraphResolver,
}

impl PreparedHistory {
    pub(crate) async fn project_accepted_checkpoints(
        &mut self,
        store: &CrudStore,
        workspace: &str,
        thread: &str,
    ) -> Result<()> {
        super::checkpoint::project_accepted_checkpoints_with_resolver(
            store,
            workspace,
            thread,
            &self.accepted_scopes,
            &mut self.messages,
            &mut self.checkpoint_graphs,
        )
        .await
    }
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
    let mut checkpoint_graphs = super::coverage::CheckpointGraphResolver::default();
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
    let includes_parent_summary = policy.is_none_or(|policy| {
        policy.include_parent_summary
            && !matches!(
                policy.mode,
                pioneer_protocol::TaskAgentContextMode::Empty
                    | pioneer_protocol::TaskAgentContextMode::Custom
            )
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
    let owner = super::native::native_owner(workspace, thread);
    let captured_head = if !omits_history && includes_parent_summary {
        Some(match outputs {
            Some(outputs) => outputs.checkpoint.clone(),
            None => store.compaction_head(&owner).await?,
        })
    } else {
        None
    };
    // Capture the head before the raw append fence. A checkpoint published
    // after this point cannot be substituted into this preparation; appends
    // between these reads remain ordinary uncovered tail history.
    let fence = match outputs {
        Some(outputs) => outputs.fence.clone(),
        None => store.compaction_history_read_fence().await?,
    };
    let projection_head = if let Some(Some(head)) = captured_head.as_ref() {
        Some(
            select_captured_head_before_excluded_turn(
                store,
                workspace,
                thread,
                &owner,
                head,
                excluded_turn,
                &mut checkpoint_graphs,
            )
            .await?,
        )
    } else {
        captured_head.clone()
    };
    let mut covered_history = BTreeSet::new();
    let mut covered_item_aliases = BTreeSet::new();
    let mut covered_event_input_evidence = BTreeMap::new();
    if let Some(Some(head)) = &projection_head
        && let Some(root) = store
            .compaction_checkpoint_source(workspace, thread, head)
            .await?
        && let Some(graph) = checkpoint_graphs
            .resolve(&store, workspace, None, &root)
            .await?
    {
        covered_history.extend(graph.leaves.iter().cloned());
        covered_history.extend(graph.replay_aliases.keys().cloned());
        covered_item_aliases.extend(graph.replay_aliases.iter().filter_map(
            |(replay, covered_source)| {
                let turn = covered_source.source.scope.strip_prefix("item:")?;
                (replay.thread == covered_source.thread).then(|| {
                    (
                        covered_source.thread.clone(),
                        turn.to_owned(),
                        covered_source.source.id.clone(),
                    )
                })
            },
        ));
        covered_item_aliases.extend(graph.replay_item_aliases.iter().cloned());
        covered_event_input_evidence.extend(
            graph
                .event_input_evidence
                .iter()
                .map(|(source, role)| (source.clone(), role.clone())),
        );
    }
    let mut messages = if omits_history {
        Vec::new()
    } else if covered_history.iter().any(|leaf| leaf.thread == thread) {
        super::history::load_task_line_history_excluding(
            &store,
            workspace,
            thread,
            excluded_turn,
            &fence,
            super::history::HistoryCoverageSelection {
                sources: &covered_history,
                item_aliases: &covered_item_aliases,
                event_input_evidence: &covered_event_input_evidence,
            },
        )
        .await?
    } else {
        super::history::load_task_line_history(&store, workspace, thread, excluded_turn, &fence)
            .await?
    };
    let mut allowed = BTreeSet::from([thread.to_owned()]);
    let mut epochs = outputs
        .map(|outputs| outputs.source_epochs.clone())
        .unwrap_or_else(|| BTreeMap::from([(thread.to_owned(), epoch)]));
    let mut accepted_turn = basis_turn.map(str::to_owned);
    let mut imports = BTreeMap::<ScopedHistorySource, Vec<PreparedFrozenImport>>::new();
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
        let basis_scopes = accepted_history_scopes_prepared(
            &store,
            workspace,
            &basis.parent_thread,
            &basis.history_json,
        )
        .await?;
        allowed.extend(basis_scopes);
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
        let legacy_basis = basis.history_json.trim_start().starts_with('[');
        let mut retained_imports = None;
        let mut projected_imports = Vec::new();
        let mut inherited = if legacy_basis {
            crate::turn_runtime_snapshot::restore_history_json(
                &store,
                workspace,
                &allowed,
                &basis.history_json,
            )
            .await?
        } else {
            let descriptor: FrozenHistoryRef = serde_json::from_str(&basis.history_json)?;
            let restored = restore_accepted_execution_basis_prepared(
                &store,
                workspace,
                &basis.parent_thread,
                thread,
                &allowed,
                &descriptor,
                &covered_history,
                &mut checkpoint_graphs,
            )
            .await?;
            retained_imports = Some(restored.retained_imports);
            projected_imports = restored.projected_imports;
            restored.messages
        };
        if legacy_basis {
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
        if legacy_basis {
            hydrate_accepted_own(
                &store,
                workspace,
                &basis.parent_thread,
                &basis.history_json,
                thread,
                &mut inherited,
            )
            .await?;
        }
        // Hydration promotes only explicitly accepted own imports. Preserve
        // that evidence when recapturing the child; provenance alone is not a grant.
        if !legacy_basis {
            let turn = accepted_turn
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("accepted execution turn missing"))?;
            for retained in retained_imports.take().expect("nonlegacy basis imports") {
                if let Some(target) = retained.checkpoint_target {
                    let prepared = store
                        .compaction_prepare_accepted_checkpoint_imports(
                            workspace,
                            thread,
                            turn,
                            &retained.import_ordinals,
                            &target.thread,
                            &target.source,
                        )
                        .await?;
                    imports.entry(target).or_default().extend(prepared);
                } else {
                    for ordinal in retained.import_ordinals {
                        let prepared = store
                            .compaction_prepare_accepted_import(workspace, thread, turn, ordinal)
                            .await?;
                        imports
                            .entry(ScopedHistorySource {
                                thread: prepared.source_thread().into(),
                                source: prepared.source().clone(),
                            })
                            .or_default()
                            .push(prepared);
                    }
                }
            }
            for projected in projected_imports {
                let prepared = store
                    .compaction_prepare_accepted_checkpoint_imports(
                        workspace,
                        thread,
                        turn,
                        &projected.import_ordinals,
                        &projected.target.thread,
                        &projected.target.source,
                    )
                    .await?;
                imports
                    .entry(projected.target.clone())
                    .or_default()
                    .extend(prepared);
            }
        }
        messages = compose_frozen_basis_with_resolver(
            &store,
            workspace,
            thread,
            &allowed,
            &inherited,
            &messages,
            &mut checkpoint_graphs,
        )
        .await?;
    }
    let mut own_outputs = BTreeMap::<ScopedHistorySource, (usize, u64)>::new();
    // Only roots produced while projecting a delivered output may consume its
    // delivery grants. Parent summaries and accepted-basis checkpoints have
    // independent provenance, even when their historical leaves overlap.
    let mut delivery_replacements =
        BTreeMap::<ScopedHistorySource, BTreeMap<usize, Vec<DeliveryCheckpointImportSource>>>::new(
        );
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
            let RestoredFrozenSelection {
                messages: mut imported,
                original_ordinals,
                mut boundary_messages,
                boundary_original_ordinals,
                model_ordinals,
            } = restore_frozen_excluding_coverage(
                &store,
                workspace,
                &branch.snapshot.output.source_thread,
                &allowed,
                &branch.snapshot.output.history,
                FrozenCoverageSelection {
                    sources: &covered_history,
                    event_input_evidence: &covered_event_input_evidence,
                },
                &mut checkpoint_graphs,
            )
            .await?;
            ensure!(
                imported.len() == original_ordinals.len(),
                "filtered Task output lost its immutable ordinals"
            );
            ensure!(
                boundary_messages.len() == boundary_original_ordinals.len(),
                "Task output boundary lost its immutable ordinals"
            );
            let layout = pioneer_agent::compaction::history::NativeHistoryLayout::from_messages(
                workspace,
                &branch.snapshot.output.source_thread,
                &boundary_messages,
                &vec![0; boundary_messages.len()],
            )?;
            for (unit, indexes) in layout.units.iter().zip(&layout.message_indexes) {
                if unit.role != pioneer_compaction::SourceRole::Own
                    || !unit.complete
                    || unit.protected_input
                {
                    continue;
                }
                for index in indexes {
                    let origin = boundary_messages[*index]
                        .provenance
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("output origin is missing"))?;
                    for reference in &origin.sources {
                        let reference = source(reference);
                        let key = ScopedHistorySource {
                            thread: origin.thread_id.clone(),
                            source: reference,
                        };
                        own_outputs
                            .entry(key.clone())
                            .or_insert((branch_index, boundary_original_ordinals[*index]));
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
            for message in &mut boundary_messages {
                let origin = message.provenance.as_mut().ok_or_else(|| {
                    anyhow::anyhow!("accepted Task output has no source identity")
                })?;
                origin.context_thread = Some(thread.into());
                if !origin.inherited && logical.is_some() {
                    origin.logical_turn_id = logical.clone();
                }
            }
            let mut projected_replacements = BTreeMap::new();
            super::checkpoint::project_accepted_checkpoints_with_boundary_evidence(
                &store,
                workspace,
                thread,
                &allowed,
                &mut imported,
                Some(super::checkpoint::ProjectionBoundaryEvidence {
                    messages: &boundary_messages,
                    model_ordinals: &model_ordinals,
                }),
                Some(&mut projected_replacements),
                &mut checkpoint_graphs,
            )
            .await?;
            for (root, selected) in projected_replacements {
                let mut grants = BTreeSet::new();
                for index in selected {
                    let origin = boundary_messages[index]
                        .provenance
                        .as_ref()
                        .ok_or_else(|| {
                            anyhow::anyhow!("projected output lost its source origin")
                        })?;
                    for reference in &origin.sources {
                        grants.insert((
                            boundary_original_ordinals[index],
                            origin.thread_id.clone(),
                            source(reference),
                        ));
                    }
                }
                delivery_replacements.entry(root).or_default().insert(
                    branch_index,
                    grants
                        .into_iter()
                        .map(|(output_ordinal, source_thread, source)| {
                            DeliveryCheckpointImportSource {
                                output_ordinal,
                                source_thread,
                                source,
                            }
                        })
                        .collect(),
                );
            }
            // The delivered text is the transport copy of this exact output.
            // Denied branches never reach here and retain that disclosed text.
            messages = remove_delivered_projection_with_resolver(
                &store,
                workspace,
                thread,
                &allowed,
                messages,
                &branch.acknowledgements,
                &mut checkpoint_graphs,
            )
            .await?;
            messages = compose_frozen_basis_with_resolver(
                &store,
                workspace,
                thread,
                &allowed,
                &messages,
                &imported,
                &mut checkpoint_graphs,
            )
            .await?;
        }
    }
    if includes_parent_summary
        && let Some(head) = projection_head.as_ref().and_then(|head| head.clone())
    {
        super::checkpoint::project_compatible_checkpoint_with_resolver(
            &store,
            super::checkpoint::ProjectionContext {
                workspace,
                context_thread: thread,
                source_thread: thread,
                owner: &owner,
                allowed: &allowed,
                allow_historical_gaps: projection_head.is_some(),
            },
            head.as_str(),
            &mut messages,
            &mut checkpoint_graphs,
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
                if let Some(grant) = own_outputs.get(&key) {
                    let (branch_index, ordinal) = *grant;
                    let branch = &outputs.branches[branch_index];
                    let prepared = store
                        .compaction_prepare_frozen_import(
                            workspace,
                            thread,
                            &branch.snapshot.delivery_id,
                            &branch.acknowledgement,
                            ordinal,
                            &key.thread,
                            &key.source,
                        )
                        .await?;
                    imports.entry(key).or_default().push(prepared);
                } else if let Some(branches) = delivery_replacements.get(&key)
                    && !imports.contains_key(&key)
                {
                    let graph = checkpoint_graphs
                        .resolve(&store, workspace, Some(&allowed), &key.source)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("checkpoint root is unavailable"))?;
                    ensure!(
                        !graph.leaves.is_empty(),
                        "checkpoint replacement has no delivered output leaves"
                    );
                    let mut selected_branch = None;
                    for (branch_index, sources) in branches {
                        let mut represented = BTreeSet::new();
                        for grant in sources {
                            let scoped = ScopedHistorySource {
                                thread: grant.source_thread.clone(),
                                source: grant.source.clone(),
                            };
                            if grant.source.scope.starts_with("checkpoint:") {
                                let granted_graph = checkpoint_graphs
                                    .resolve(&store, workspace, Some(&allowed), &grant.source)
                                    .await?
                                    .ok_or_else(|| {
                                        anyhow::anyhow!("granted checkpoint is unavailable")
                                    })?;
                                represented.extend(granted_graph.leaves.iter().cloned());
                            } else {
                                represented.insert(scoped);
                            }
                        }
                        if represented == graph.leaves {
                            selected_branch = Some((*branch_index, sources));
                            break;
                        }
                    }
                    let (branch_index, sources) = selected_branch.ok_or_else(|| {
                        anyhow::anyhow!(
                            "checkpoint replacement exceeds each accepted Task delivery"
                        )
                    })?;
                    let branch = &outputs.branches[branch_index];
                    let prepared = store
                        .compaction_prepare_delivery_checkpoint_imports(
                            workspace,
                            thread,
                            &branch.snapshot.delivery_id,
                            &branch.acknowledgement,
                            sources,
                            &key.thread,
                            &key.source,
                        )
                        .await?;
                    imports.entry(key).or_default().extend(prepared);
                }
            }
        }
    }
    let mut prepared = capture_with_imports_prepared(
        &store,
        workspace,
        thread,
        &allowed,
        &messages,
        &imports,
        checkpoint_graphs,
    )
    .await?;

    for (source_thread, expected) in &epochs {
        ensure!(
            store
                .compaction_projection_version(workspace, source_thread)
                .await?
                == *expected,
            "parent history changed while freezing the accepted context"
        );
    }
    prepared.source_epochs = prepared_source_epochs(&prepared.accepted_scopes, &epochs)?;
    prepared.expected_checkpoint = captured_head.flatten();
    prepared.checkpoint = projection_head.flatten();
    Ok(prepared)
}

/// A head captured before the append fence is temporally bounded, but a caller
/// may also exclude the currently executing turn. Select an older whole
/// checkpoint when saved coverage crosses that explicit boundary; this is a
/// metadata-only check and never restores the excluded payload.
async fn select_captured_head_before_excluded_turn(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    owner: &str,
    head: &str,
    excluded_turn: Option<&str>,
    checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
) -> Result<Option<String>> {
    let Some(excluded_turn) = excluded_turn else {
        return Ok(Some(head.to_owned()));
    };
    let mut candidate = Some(head.to_owned());
    let mut seen = BTreeSet::new();
    while let Some(id) = candidate {
        ensure!(seen.insert(id.clone()), "cyclic checkpoint ancestry");
        ensure!(
            seen.len() <= 65_536,
            "checkpoint ancestry exceeds supported quantum"
        );
        let edges = checkpoint_graphs
            .ancestry_edges(store, workspace, &id)
            .await?;
        ensure!(
            edges.owner == owner && edges.thread_id == thread,
            "checkpoint belongs to another context"
        );
        let next = edges.previous.clone();
        let Some(root) = store
            .compaction_checkpoint_source(workspace, thread, &id)
            .await?
        else {
            candidate = next;
            continue;
        };
        let graph = checkpoint_graphs
            .resolve(store, workspace, None, &root)
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint root is unavailable"))?;
        let crosses_excluded_turn = graph.leaves.iter().any(|leaf| {
            leaf.thread == thread
                && leaf
                    .source
                    .scope
                    .split_once(':')
                    .is_some_and(|(_, turn)| turn == excluded_turn)
        });
        if !crosses_excluded_turn {
            return Ok(Some(id));
        }
        candidate = next;
    }
    Ok(None)
}

pub(super) fn prepared_source_epochs(
    accepted_scopes: &BTreeSet<String>,
    captured_epochs: &BTreeMap<String, u64>,
) -> Result<BTreeMap<String, u64>> {
    accepted_scopes
        .iter()
        .map(|source_thread| {
            captured_epochs
                .get(source_thread)
                .copied()
                .map(|epoch| (source_thread.clone(), epoch))
                .ok_or_else(|| anyhow::anyhow!("prepared manifest scope lost its source epoch"))
        })
        .collect()
}

/// Remove only identified raw transport copies. A summary is atomic: text that
/// already summarizes a delivery is retained and is never rematerialized.
#[cfg(test)]
pub(super) async fn remove_delivered_projection(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    allowed: &BTreeSet<String>,
    messages: Vec<ChatMessage>,
    acknowledgements: &[SourceRef],
) -> Result<Vec<ChatMessage>> {
    let mut checkpoint_graphs = super::coverage::CheckpointGraphResolver::default();
    remove_delivered_projection_with_resolver(
        store,
        workspace,
        thread,
        allowed,
        messages,
        acknowledgements,
        &mut checkpoint_graphs,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn remove_delivered_projection_with_resolver(
    _store: &CrudStore,
    _workspace: &str,
    thread: &str,
    _allowed: &BTreeSet<String>,
    messages: Vec<ChatMessage>,
    acknowledgements: &[SourceRef],
    _checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
) -> Result<Vec<ChatMessage>> {
    let copies = acknowledgements.iter().cloned().collect::<BTreeSet<_>>();
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
    Ok(messages
        .into_iter()
        .filter(|message| {
            !message
                .provenance
                .as_ref()
                .unwrap()
                .sources
                .iter()
                .any(|reference| copies.contains(&source(reference)))
        })
        .collect())
}

#[cfg(test)]
pub(super) async fn compose_frozen_basis(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    allowed: &BTreeSet<String>,
    inherited: &[ChatMessage],
    own: &[ChatMessage],
) -> Result<Vec<ChatMessage>> {
    let mut checkpoint_graphs = super::coverage::CheckpointGraphResolver::default();
    compose_frozen_basis_with_resolver(
        store,
        workspace,
        thread,
        allowed,
        inherited,
        own,
        &mut checkpoint_graphs,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn compose_frozen_basis_with_resolver(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    allowed: &BTreeSet<String>,
    inherited: &[ChatMessage],
    own: &[ChatMessage],
    checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
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
                    let graph = checkpoint_graphs
                        .resolve(store, workspace, Some(allowed), &source)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("checkpoint root is unavailable"))?;
                    checkpoints.insert(key, graph.leaves.clone());
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
        let Some(overlap) =
            error.downcast_ref::<pioneer_agent::compaction::composition::SplitRawInputsRequired>()
        else {
            return Err(error);
        };
        let split_inherited = super::compatible::split_raw_overlap(
            store,
            workspace,
            allowed,
            &inherited,
            &overlap.affected,
        )
        .await?;
        let split_own = super::compatible::split_raw_overlap(
            store,
            workspace,
            allowed,
            &own,
            &overlap.affected,
        )
        .await?;
        ensure!(
            split_inherited != inherited || split_own != own,
            "overlapping raw units cannot be normalized as canonical inputs"
        );
        inherited = split_inherited;
        own = split_own;
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

pub(super) fn select_task_history(
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
        super::coverage::CheckpointGraphResolver::default(),
    )
    .await?
    .descriptor)
}

struct CaptureRenderer {
    event_input_roles: bool,
    checkpoint_graphs: super::coverage::CheckpointGraphResolver,
}

/// Build the exact reference identity used before frozen manifests recorded
/// event-input roles. Upgrade tests use this to exercise an installed manifest
/// rather than a current manifest whose optional role merely happens to be
/// absent.
#[cfg(test)]
pub(crate) async fn capture_legacy_event_references(
    store: &CrudStore,
    workspace: &str,
    owner_thread: &str,
    allowed_threads: &BTreeSet<String>,
    messages: &[ChatMessage],
) -> Result<FrozenHistoryRef> {
    Ok(capture_with_imports_prepared_using_renderer(
        store,
        workspace,
        owner_thread,
        allowed_threads,
        messages,
        &BTreeMap::new(),
        CaptureRenderer {
            event_input_roles: false,
            checkpoint_graphs: super::coverage::CheckpointGraphResolver::default(),
        },
    )
    .await?
    .descriptor)
}

pub(super) async fn capture_with_imports_prepared(
    store: &CrudStore,
    workspace: &str,
    owner_thread: &str,
    allowed_threads: &BTreeSet<String>,
    messages: &[ChatMessage],
    imports: &BTreeMap<ScopedHistorySource, Vec<PreparedFrozenImport>>,
    checkpoint_graphs: super::coverage::CheckpointGraphResolver,
) -> Result<PreparedHistory> {
    capture_with_imports_prepared_using_renderer(
        store,
        workspace,
        owner_thread,
        allowed_threads,
        messages,
        imports,
        CaptureRenderer {
            event_input_roles: true,
            checkpoint_graphs,
        },
    )
    .await
}

async fn capture_with_imports_prepared_using_renderer(
    store: &CrudStore,
    workspace: &str,
    owner_thread: &str,
    allowed_threads: &BTreeSet<String>,
    messages: &[ChatMessage],
    imports: &BTreeMap<ScopedHistorySource, Vec<PreparedFrozenImport>>,
    renderer: CaptureRenderer,
) -> Result<PreparedHistory> {
    let mut checkpoint_graphs = renderer.checkpoint_graphs;
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
            event_input_role: None,
            inherited: origin.inherited,
            complete: origin.complete,
            protected_input: origin.protected_input,
            wire_sha256: wire_digest(message)?,
            replay_source: None,
            tool_item_id: None,
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
            reference.tool_item_id = Some(item.item_id().to_owned());
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
        references.push(reference);
    }
    if renderer.event_input_roles {
        let mut event_projections = BTreeMap::new();
        let mut event_sources = BTreeMap::<String, BTreeSet<SourceRef>>::new();
        for reference in &references {
            if let [source] = reference.sources.as_slice()
                && source.scope.starts_with("event:")
            {
                event_sources
                    .entry(reference.source_thread.clone())
                    .or_default()
                    .insert(source.clone());
            }
        }
        for (thread, sources) in event_sources {
            for projection in
                super::history::historical_event_projections(store, workspace, &thread, sources)
                    .await?
            {
                let role = match projection.projection_kind.as_str() {
                    "input" | "input_revision" => {
                        pioneer_compaction::frozen::FrozenEventInputRole::Authoritative
                    }
                    "input_deleted" => pioneer_compaction::frozen::FrozenEventInputRole::Deleted,
                    "input_copy" => pioneer_compaction::frozen::FrozenEventInputRole::InputCopy,
                    _ => continue,
                };
                event_projections.insert((thread.clone(), projection.reference), role);
            }
        }
        for reference in &mut references {
            if let [source] = reference.sources.as_slice() {
                reference.event_input_role = event_projections
                    .get(&(reference.source_thread.clone(), source.clone()))
                    .copied();
            }
        }
    }
    for reference in &references {
        reference.validate()?;
        digest_entry(&mut digest, reference)?;
    }
    let mut verified_messages = Vec::with_capacity(messages.len());
    let mut restore_state = FrozenExecutionRestoreState::from_references(
        store,
        workspace,
        allowed_threads,
        &references,
        &mut checkpoint_graphs,
    )
    .await?;
    for page in references.chunks(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize) {
        let authenticated = restore_entries_page(
            store,
            workspace,
            allowed_threads,
            page,
            &mut checkpoint_graphs,
        )
        .await?;
        ensure!(
            authenticated.len() == page.len(),
            "frozen history count mismatch"
        );
        let restored = restore_execution_entries_page(
            store,
            workspace,
            allowed_threads,
            page,
            &mut checkpoint_graphs,
            &mut restore_state,
        )
        .await?;
        ensure!(
            restored.iter().all(Option::is_some),
            "new frozen history contains a non-model event"
        );
        verified_messages.extend(restored.into_iter().flatten());
    }
    let accepted_scopes = prepared_manifest_scopes(
        store,
        workspace,
        owner_thread,
        &references,
        &mut checkpoint_graphs,
    )
    .await?;
    let mut accepted = Vec::new();
    for (ordinal, reference) in references.iter().enumerate() {
        for source in &reference.sources {
            if let Some(prepared) = imports.get(&ScopedHistorySource {
                thread: reference.source_thread.clone(),
                source: source.clone(),
            }) {
                accepted.extend(
                    prepared
                        .iter()
                        .cloned()
                        .map(|prepared| (ordinal as u64, prepared)),
                );
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
                accepted_scopes,
                source_epochs: BTreeMap::new(),
                expected_checkpoint: None,
                checkpoint: None,
                checkpoint_graphs,
            });
        }
        let messages = restore_model_with_resolver(
            store,
            workspace,
            allowed_threads,
            &existing,
            &mut checkpoint_graphs,
        )
        .await?;
        return Ok(PreparedHistory {
            descriptor: existing,
            messages,
            accepted_scopes,
            source_epochs: BTreeMap::new(),
            expected_checkpoint: None,
            checkpoint: None,
            checkpoint_graphs,
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
        accepted_scopes,
        source_epochs: BTreeMap::new(),
        expected_checkpoint: None,
        checkpoint: None,
        checkpoint_graphs,
    })
}

async fn prepared_manifest_scopes(
    _store: &CrudStore,
    _workspace: &str,
    owner_thread: &str,
    references: &[FrozenMessageRef],
    _checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
) -> Result<BTreeSet<String>> {
    let mut scopes = BTreeSet::from([owner_thread.to_owned()]);
    for reference in references {
        scopes.insert(reference.source_thread.clone());
        scopes.extend(reference.context_thread.iter().cloned());
    }
    Ok(scopes)
}

pub(crate) async fn restore(
    store: &CrudStore,
    workspace: &str,
    allowed_threads: &BTreeSet<String>,
    descriptor: &FrozenHistoryRef,
) -> Result<Vec<ChatMessage>> {
    let mut checkpoint_graphs = super::coverage::CheckpointGraphResolver::default();
    restore_with_resolver(
        store,
        workspace,
        allowed_threads,
        descriptor,
        &mut checkpoint_graphs,
    )
    .await
}

/// Read and verify only an immutable frozen manifest. Payload restoration is a
/// separate step so execution preparation can replace covered entries before
/// touching their canonical bodies.
async fn frozen_manifest_references(
    store: &CrudStore,
    workspace: &str,
    expected_owner: Option<&str>,
    descriptor: &FrozenHistoryRef,
    allowed_threads: Option<&BTreeSet<String>>,
) -> Result<(String, Vec<FrozenMessageRef>)> {
    let owner = store
        .compaction_frozen_history_owner(workspace, descriptor)
        .await?
        .ok_or_else(|| anyhow::anyhow!("frozen history manifest is unavailable or incomplete"))?;
    ensure!(
        expected_owner.is_none_or(|expected| owner == expected),
        "frozen history manifest owner changed"
    );
    if let Some(allowed) = allowed_threads {
        ensure!(
            allowed.contains(&owner),
            "frozen history owner is outside the accepted context"
        );
    }
    let mut references = Vec::new();
    let mut digest = Sha256::new();
    while (references.len() as u64) < descriptor.messages {
        let page = store
            .compaction_frozen_history_page(
                workspace,
                &owner,
                &descriptor.manifest_id,
                references.len() as u64,
            )
            .await?;
        ensure!(
            !page.is_empty(),
            "frozen history lost an immutable reference page"
        );
        for reference in &page {
            reference.validate()?;
            if let Some(allowed) = allowed_threads {
                ensure!(
                    allowed.contains(&reference.source_thread)
                        && reference
                            .context_thread
                            .as_ref()
                            .is_none_or(|owner| allowed.contains(owner)),
                    "frozen history source is outside the accepted context"
                );
            }
            digest_entry(&mut digest, reference)?;
        }
        references.extend(page);
        ensure!(
            references.len() as u64 <= descriptor.messages,
            "frozen history manifest count mismatch"
        );
    }
    ensure!(
        references.len() as u64 == descriptor.messages
            && hex::encode(digest.finalize()) == descriptor.identity_sha256,
        "frozen history manifest digest mismatch"
    );
    Ok((owner, references))
}

/// Validate that a provider continuity receipt still names the exact directly
/// consumed canonical sources, without materializing their payloads. A
/// checkpoint is checked as the independently published source in the frozen
/// manifest; covered raw leaves are intentionally not revalidated here.
#[cfg(test)]
pub(crate) async fn validate_frozen_history_current(
    store: &CrudStore,
    workspace: &str,
    owner: &str,
    history_json: &str,
) -> Result<bool> {
    if history_json.trim_start().starts_with('[') {
        anyhow::bail!("legacy inline history cannot prove provider continuity");
    }
    let descriptor: FrozenHistoryRef = serde_json::from_str(history_json)?;
    // The immutable accepted manifest is both the authority grant and the
    // source-version proof. Verify it once, then reuse its references for the
    // exact-current check below; do not re-read the manifest merely to derive
    // the same accepted scopes.
    let (_, references) =
        frozen_manifest_references(store, workspace, Some(owner), &descriptor, None).await?;
    let mut direct_sources = BTreeMap::<String, BTreeSet<pioneer_compaction::SourceRef>>::new();
    for reference in references {
        let mut sources = reference.sources;
        if let Some(replay_source) = reference.replay_source {
            sources.push(replay_source);
        }
        direct_sources
            .entry(reference.source_thread)
            .or_default()
            .extend(sources);
    }
    validate_direct_source_groups_current(store, workspace, direct_sources).await
}

/// Validate the immutable snapshot boundary and its accepted import grants
/// without treating every raw source named by that boundary as provider-visible.
/// Exact-current validation belongs to the execution projection's direct
/// sources, because a published summary may have replaced covered raw entries.
pub(crate) async fn validate_frozen_history_authority(
    store: &CrudStore,
    workspace: &str,
    owner: &str,
    history_json: &str,
) -> Result<bool> {
    if history_json.trim_start().starts_with('[') {
        anyhow::bail!("legacy inline history cannot prove provider continuity");
    }
    let descriptor: FrozenHistoryRef = serde_json::from_str(history_json)?;
    let (_, references) =
        frozen_manifest_references(store, workspace, Some(owner), &descriptor, None).await?;
    let mut checkpoint_graphs = super::coverage::CheckpointGraphResolver::default();
    let _ = read_accepted_imports(
        store,
        workspace,
        owner,
        &descriptor,
        &references,
        None,
        &mut checkpoint_graphs,
    )
    .await?;
    Ok(true)
}

pub(crate) async fn validate_direct_history_sources_current(
    store: &CrudStore,
    workspace: &str,
    sources: &[(String, pioneer_compaction::SourceRef)],
) -> Result<bool> {
    let mut direct_sources = BTreeMap::<String, BTreeSet<pioneer_compaction::SourceRef>>::new();
    for (source_thread, source) in sources {
        direct_sources
            .entry(source_thread.clone())
            .or_default()
            .insert(source.clone());
    }
    validate_direct_source_groups_current(store, workspace, direct_sources).await
}

async fn validate_direct_source_groups_current(
    store: &CrudStore,
    workspace: &str,
    direct_sources: BTreeMap<String, BTreeSet<pioneer_compaction::SourceRef>>,
) -> Result<bool> {
    for (source_thread, sources) in direct_source_batches(direct_sources)? {
        if !source_batch_is_current(store, workspace, &source_thread, &sources).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Both the ordinary continuity guard and accepted-history refresh use these
/// same row/byte quanta. The latter refines only failed quanta to identify the
/// exact accepted turns that must be projected at current revisions.
fn direct_source_batches(
    direct_sources: BTreeMap<String, BTreeSet<pioneer_compaction::SourceRef>>,
) -> Result<Vec<(String, Vec<pioneer_compaction::SourceRef>)>> {
    let mut batches = Vec::new();
    for (source_thread, sources) in direct_sources {
        let sources = sources.into_iter().collect::<Vec<_>>();
        let mut start = 0;
        while start < sources.len() {
            let mut end = start;
            let mut bytes = 2_usize;
            while end < sources.len()
                && end - start < pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize
            {
                let encoded = serde_json::to_vec(&sources[end])?;
                let separator = usize::from(end > start);
                if end > start
                    && bytes
                        .saturating_add(separator)
                        .saturating_add(encoded.len())
                        > pioneer_crud::compaction::SOURCE_PAGE_BYTES
                {
                    break;
                }
                ensure!(
                    encoded.len().saturating_add(2) <= pioneer_crud::compaction::SOURCE_PAGE_BYTES,
                    "provider continuity source reference exceeds validation bound"
                );
                bytes = bytes
                    .saturating_add(separator)
                    .saturating_add(encoded.len());
                end += 1;
            }
            batches.push((source_thread.clone(), sources[start..end].to_vec()));
            start = end;
        }
    }
    Ok(batches)
}

async fn source_batch_is_current(
    store: &CrudStore,
    workspace: &str,
    source_thread: &str,
    sources: &[pioneer_compaction::SourceRef],
) -> Result<bool> {
    let current = store
        .compaction_sources_current(workspace, source_thread, sources)
        .await?;
    #[cfg(test)]
    if let Some(calls) = CONTINUITY_SOURCE_LOOKUPS
        .lock()
        .unwrap()
        .get(&(
            store.database_connection().runtime_identity(),
            workspace.to_owned(),
        ))
        .and_then(std::sync::Weak::upgrade)
    {
        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    Ok(current)
}

pub(super) async fn stale_direct_sources(
    store: &CrudStore,
    workspace: &str,
    direct_sources: BTreeMap<String, BTreeSet<pioneer_compaction::SourceRef>>,
) -> Result<BTreeSet<(String, pioneer_compaction::SourceRef)>> {
    let mut stale = BTreeSet::new();
    for (thread, batch) in direct_source_batches(direct_sources)? {
        let mut pending = vec![batch];
        while let Some(sources) = pending.pop() {
            if source_batch_is_current(store, workspace, &thread, &sources).await? {
                continue;
            }
            if sources.len() == 1 {
                stale.insert((thread.clone(), sources.into_iter().next().unwrap()));
                continue;
            }
            let middle = sources.len() / 2;
            pending.push(sources[middle..].to_vec());
            pending.push(sources[..middle].to_vec());
        }
    }
    Ok(stale)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HistoricalFrozenIdentity {
    thread: String,
    scope: String,
    id: String,
}

/// Version-free only for selecting whole messages after exact historical
/// membership has admitted a checkpoint. It is never the snapshot-boundary
/// comparison used to decide whether a later checkpoint is compatible.
fn historical_frozen_identity(
    source: &pioneer_agent::compaction::composition::ScopedHistorySource,
) -> HistoricalFrozenIdentity {
    HistoricalFrozenIdentity {
        thread: source.thread.clone(),
        scope: source.source.scope.clone(),
        id: source.source.id.clone(),
    }
}

async fn frozen_reference_leaves(
    store: &CrudStore,
    workspace: &str,
    allowed: &BTreeSet<String>,
    reference: &FrozenMessageRef,
    checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
) -> Result<BTreeSet<ScopedHistorySource>> {
    let mut leaves = BTreeSet::new();
    for source in &reference.sources {
        if source.scope.starts_with("checkpoint:") {
            let graph = checkpoint_graphs
                .resolve(store, workspace, Some(allowed), source)
                .await?
                .ok_or_else(|| anyhow::anyhow!("checkpoint root is unavailable"))?;
            leaves.extend(graph.leaves.iter().cloned());
        } else {
            leaves.insert(ScopedHistorySource {
                thread: reference.source_thread.clone(),
                source: source.clone(),
            });
        }
    }
    Ok(leaves)
}

struct RestoredFrozenSelection {
    messages: Vec<ChatMessage>,
    original_ordinals: Vec<u64>,
    boundary_messages: Vec<ChatMessage>,
    boundary_original_ordinals: Vec<u64>,
    model_ordinals: Vec<usize>,
}

struct FrozenCoverageSelection<'a> {
    sources: &'a BTreeSet<ScopedHistorySource>,
    event_input_evidence: &'a BTreeMap<ScopedHistorySource, String>,
}

/// Verify an immutable manifest and its import proofs at original ordinals,
/// then restore only messages not wholly represented by an already accepted
/// checkpoint. This is an execution-preparation projection; literal restore
/// keeps its exact count/order behavior.
async fn restore_frozen_excluding_coverage(
    store: &CrudStore,
    workspace: &str,
    owner: &str,
    allowed: &BTreeSet<String>,
    descriptor: &FrozenHistoryRef,
    coverage: FrozenCoverageSelection<'_>,
    checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
) -> Result<RestoredFrozenSelection> {
    let (_, references) =
        frozen_manifest_references(store, workspace, Some(owner), descriptor, Some(allowed))
            .await?;
    let _accepted = read_accepted_imports(
        store,
        workspace,
        owner,
        descriptor,
        &references,
        None,
        checkpoint_graphs,
    )
    .await?;
    let mut selected = BTreeSet::new();
    if !coverage.sources.is_empty() {
        for (ordinal, reference) in references.iter().enumerate() {
            let leaves =
                frozen_reference_leaves(store, workspace, allowed, reference, checkpoint_graphs)
                    .await?;
            if !leaves.is_empty() && leaves.is_subset(coverage.sources) {
                selected.insert(ordinal);
            }
        }
        let mut units = BTreeMap::<(String, String), Vec<usize>>::new();
        for (ordinal, reference) in references.iter().enumerate() {
            units
                .entry((reference.source_thread.clone(), reference.unit_id.clone()))
                .or_default()
                .push(ordinal);
        }
        for unit in units.values() {
            if unit.iter().any(|ordinal| selected.contains(ordinal))
                && !unit.iter().all(|ordinal| selected.contains(ordinal))
            {
                for ordinal in unit {
                    selected.remove(ordinal);
                }
            }
        }
    }
    let mut restore_state = FrozenExecutionRestoreState::from_references(
        store,
        workspace,
        allowed,
        &references,
        checkpoint_graphs,
    )
    .await?;
    restore_state.extend_input_coverage(coverage.sources.iter());
    restore_state.extend_event_input_evidence(coverage.event_input_evidence);
    let retained = references
        .into_iter()
        .enumerate()
        .filter_map(|(ordinal, reference)| {
            (!selected.contains(&ordinal)).then_some((ordinal as u64, reference))
        })
        .collect::<Vec<_>>();
    let mut messages = Vec::with_capacity(retained.len());
    let mut original_ordinals = Vec::with_capacity(retained.len());
    let mut boundary_messages = Vec::with_capacity(retained.len());
    let mut boundary_original_ordinals = Vec::with_capacity(retained.len());
    let mut model_ordinals = Vec::with_capacity(retained.len());
    for page in retained.chunks(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize) {
        let references = page
            .iter()
            .map(|(_, reference)| reference.clone())
            .collect::<Vec<_>>();
        let restored = restore_execution_entries_page(
            store,
            workspace,
            allowed,
            &references,
            checkpoint_graphs,
            &mut restore_state,
        )
        .await?;
        let literal =
            restore_entries_page(store, workspace, allowed, &references, checkpoint_graphs).await?;
        ensure!(
            literal.len() == page.len() && restored.len() == page.len(),
            "frozen history count mismatch after checkpoint projection"
        );
        for (((ordinal, _), message), authenticated) in page.iter().zip(restored).zip(literal) {
            let boundary_ordinal = boundary_messages.len();
            boundary_original_ordinals.push(*ordinal);
            boundary_messages.push(authenticated);
            if let Some(message) = message {
                model_ordinals.push(boundary_ordinal);
                original_ordinals.push(*ordinal);
                messages.push(message);
            }
        }
    }
    Ok(RestoredFrozenSelection {
        original_ordinals,
        messages,
        boundary_messages,
        boundary_original_ordinals,
        model_ordinals,
    })
}

/// Restore an accepted execution basis with compatible published summaries
/// applied before raw payload access. The immutable manifest and import grants
/// are verified at their original ordinals; the stored snapshot is not
/// rewritten and literal `restore` retains its original contract.
struct RestoredExecutionBasis {
    messages: Vec<ChatMessage>,
    direct_sources: Vec<ScopedHistorySource>,
    retained_imports: Vec<RetainedAcceptedImports>,
    projected_imports: Vec<ProjectedAcceptedImports>,
}

struct RetainedAcceptedImports {
    checkpoint_target: Option<ScopedHistorySource>,
    import_ordinals: Vec<u64>,
}

struct ProjectedAcceptedImports {
    target: ScopedHistorySource,
    import_ordinals: Vec<u64>,
}

fn accepted_source_turn<'a>(scopes: impl Iterator<Item = &'a str>) -> Option<String> {
    let mut turns = scopes.filter_map(|scope| {
        ["input:", "event:", "context:", "item:"]
            .iter()
            .find_map(|prefix| scope.strip_prefix(prefix))
    });
    let turn = turns.next()?;
    turns.all(|other| other == turn).then(|| turn.to_owned())
}

fn accepted_reference_source_turn(reference: &FrozenMessageRef) -> Option<String> {
    accepted_source_turn(reference.sources.iter().map(|source| source.scope.as_str()))
}

fn accepted_provenance_source_turn(origin: &MessageProvenance) -> Option<String> {
    accepted_source_turn(origin.sources.iter().map(|source| source.scope.as_str()))
}

async fn restore_accepted_execution_basis_prepared(
    store: &CrudStore,
    workspace: &str,
    parent: &str,
    execution_thread: &str,
    allowed: &BTreeSet<String>,
    descriptor: &FrozenHistoryRef,
    externally_covered: &BTreeSet<ScopedHistorySource>,
    checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
) -> Result<RestoredExecutionBasis> {
    struct ExecutionProjection {
        anchor: usize,
        source_thread: String,
        owner: String,
        checkpoint: String,
        checkpoint_source: SourceRef,
        selected: BTreeSet<usize>,
        coverage: BTreeSet<ScopedHistorySource>,
        event_input_evidence: BTreeMap<ScopedHistorySource, String>,
        inherited: bool,
        import_ordinals: Vec<u64>,
    }

    let (_, references) =
        frozen_manifest_references(store, workspace, Some(parent), descriptor, Some(allowed))
            .await?;
    let accepted = read_accepted_imports(
        store,
        workspace,
        parent,
        descriptor,
        &references,
        Some(execution_thread),
        checkpoint_graphs,
    )
    .await?;
    let effective = |ordinal: usize, reference: &FrozenMessageRef| {
        if accepted.contains_key(&ordinal) {
            (false, execution_thread.to_owned())
        } else {
            (
                reference.inherited,
                reference
                    .context_thread
                    .as_deref()
                    .unwrap_or(&reference.source_thread)
                    .to_owned(),
            )
        }
    };
    let mut source_threads = references
        .iter()
        .enumerate()
        .filter_map(|(ordinal, reference)| {
            let (inherited, context_owner) = effective(ordinal, reference);
            (reference.source_thread != execution_thread
                && (inherited || context_owner == execution_thread))
                .then(|| reference.source_thread.clone())
        })
        .collect::<BTreeSet<_>>();
    // A restart may happen after this execution published a working-context
    // checkpoint over its accepted basis. It is safe to consider that head:
    // the exact historical-membership test below rejects any later work that
    // was not represented by the immutable manifest.
    source_threads.insert(execution_thread.to_owned());
    let mut omitted = BTreeSet::new();
    if !externally_covered.is_empty() {
        let mut externally_selected = BTreeSet::new();
        for (ordinal, reference) in references.iter().enumerate() {
            let leaves =
                frozen_reference_leaves(store, workspace, allowed, reference, checkpoint_graphs)
                    .await?;
            if !leaves.is_empty() && leaves.is_subset(externally_covered) {
                externally_selected.insert(ordinal);
            }
        }
        let mut units = BTreeMap::<(String, String), Vec<usize>>::new();
        for (ordinal, reference) in references.iter().enumerate() {
            units
                .entry((reference.source_thread.clone(), reference.unit_id.clone()))
                .or_default()
                .push(ordinal);
        }
        for unit in units.values() {
            if unit
                .iter()
                .any(|ordinal| externally_selected.contains(ordinal))
                && unit
                    .iter()
                    .all(|ordinal| externally_selected.contains(ordinal))
            {
                omitted.extend(unit.iter().copied());
            }
        }
    }
    let mut projections = Vec::<ExecutionProjection>::new();
    for source_thread in source_threads {
        ensure!(
            allowed.contains(&source_thread),
            "checkpoint source scope is not accepted"
        );
        let owner = super::native::native_owner(workspace, &source_thread);
        let mut candidate = store.compaction_head(&owner).await?;
        let mut seen = BTreeSet::new();
        while let Some(id) = candidate {
            ensure!(seen.insert(id.clone()), "cyclic checkpoint ancestry");
            ensure!(
                seen.len() <= 65_536,
                "checkpoint ancestry exceeds supported quantum"
            );
            let edges = checkpoint_graphs
                .ancestry_edges(store, workspace, &id)
                .await?;
            ensure!(
                edges.owner == owner && edges.thread_id == source_thread,
                "checkpoint belongs to another context"
            );
            let next = edges.previous.clone();
            let Some(root) = store
                .compaction_checkpoint_source(workspace, &source_thread, &id)
                .await?
            else {
                candidate = next;
                continue;
            };
            let graph = checkpoint_graphs
                .resolve(store, workspace, Some(allowed), &root)
                .await?
                .ok_or_else(|| anyhow::anyhow!("checkpoint root is unavailable"))?;
            let metadata = checkpoint_graphs
                .projection_metadata(store, workspace, &graph)
                .await?;
            let required = graph.leaves.clone();
            let covered = graph
                .leaves
                .iter()
                .chain(graph.replay_aliases.keys())
                .map(historical_frozen_identity)
                .collect::<BTreeSet<_>>();
            let emergency = metadata
                .emergency_inputs
                .iter()
                .map(historical_frozen_identity)
                .collect::<BTreeSet<_>>();
            let mut represented = BTreeSet::new();
            let mut leaves_by_ordinal = BTreeMap::new();
            for (ordinal, reference) in references.iter().enumerate() {
                if omitted.contains(&ordinal) {
                    continue;
                }
                let (inherited, context_owner) = effective(ordinal, reference);
                let replaceable = match metadata.coverage_domain {
                    pioneer_compaction::CoverageDomain::OwnContribution => {
                        !inherited && context_owner == execution_thread
                    }
                    pioneer_compaction::CoverageDomain::WorkingContext => {
                        allowed.contains(&reference.source_thread)
                            && (inherited || context_owner == execution_thread)
                    }
                };
                if !replaceable {
                    continue;
                }
                let leaves = frozen_reference_leaves(
                    store,
                    workspace,
                    allowed,
                    reference,
                    checkpoint_graphs,
                )
                .await?;
                represented.extend(leaves.iter().cloned());
                leaves_by_ordinal.insert(ordinal, leaves);
            }
            let mut represented_coverage = represented.clone();
            for (replay, covered_source) in &graph.replay_aliases {
                if represented.contains(replay) {
                    represented_coverage.insert(covered_source.clone());
                }
            }
            if required.difference(&represented_coverage).next().is_some() {
                candidate = next;
                continue;
            }
            let mut selected = BTreeSet::new();
            for (ordinal, leaves) in &leaves_by_ordinal {
                let identities = leaves
                    .iter()
                    .map(historical_frozen_identity)
                    .collect::<BTreeSet<_>>();
                if identities.is_disjoint(&covered) || !identities.is_subset(&covered) {
                    continue;
                }
                let reference = &references[*ordinal];
                ensure!(
                    reference.complete
                        && (!reference.protected_input
                            || identities.iter().all(|leaf| emergency.contains(leaf))),
                    "checkpoint cannot replace pending or protected input"
                );
                selected.insert(*ordinal);
            }
            let mut units = BTreeMap::<(String, String), Vec<usize>>::new();
            for (ordinal, reference) in references.iter().enumerate() {
                if leaves_by_ordinal.contains_key(&ordinal) {
                    units
                        .entry((reference.source_thread.clone(), reference.unit_id.clone()))
                        .or_default()
                        .push(ordinal);
                }
            }
            let splits_unit = units.values().any(|unit| {
                unit.iter().any(|ordinal| selected.contains(ordinal))
                    && !unit.iter().all(|ordinal| selected.contains(ordinal))
            });
            if splits_unit {
                candidate = next;
                continue;
            }
            let Some(anchor) = selected.first().copied() else {
                candidate = next;
                continue;
            };
            let import_ordinals = if metadata.coverage_domain
                == pioneer_compaction::CoverageDomain::OwnContribution
            {
                selected
                    .iter()
                    .filter_map(|ordinal| accepted.get(ordinal))
                    .flat_map(|imports| imports.import_ordinals.iter().copied())
                    .collect::<Vec<_>>()
            } else {
                // Working-context projections remain inherited. Their access
                // comes from the accepted basis boundary, never from OWN
                // imports attached to an inherited target.
                Vec::new()
            };
            if metadata.coverage_domain == pioneer_compaction::CoverageDomain::OwnContribution
                && source_thread != execution_thread
                && (selected
                    .iter()
                    .any(|ordinal| !accepted.contains_key(ordinal))
                    || import_ordinals.is_empty())
            {
                candidate = next;
                continue;
            }
            projections.push(ExecutionProjection {
                anchor,
                source_thread: source_thread.clone(),
                owner,
                checkpoint: id,
                checkpoint_source: root,
                selected,
                coverage: graph.leaves.clone(),
                event_input_evidence: graph.event_input_evidence.clone(),
                inherited: metadata.coverage_domain
                    == pioneer_compaction::CoverageDomain::WorkingContext,
                import_ordinals,
            });
            break;
        }
    }

    let mut keep = vec![true; projections.len()];
    for left in 0..projections.len() {
        if !keep[left] {
            continue;
        }
        for right in left + 1..projections.len() {
            if !keep[right] {
                continue;
            }
            if projections[left].checkpoint == projections[right].checkpoint {
                keep[right] = false;
                continue;
            }
            if projections[left].inherited != projections[right].inherited {
                continue;
            }
            if projections[left]
                .coverage
                .is_subset(&projections[right].coverage)
                && projections[left]
                    .selected
                    .is_subset(&projections[right].selected)
            {
                keep[left] = false;
                break;
            }
            if projections[right]
                .coverage
                .is_subset(&projections[left].coverage)
                && projections[right]
                    .selected
                    .is_subset(&projections[left].selected)
            {
                keep[right] = false;
            }
        }
    }
    projections = projections
        .into_iter()
        .zip(keep)
        .filter_map(|(projection, keep)| keep.then_some(projection))
        .collect();
    for projection in &projections {
        omitted.extend(projection.selected.iter().copied());
    }
    let retained_imports = accepted
        .iter()
        .filter(|(message_ordinal, _)| !omitted.contains(message_ordinal))
        .map(|(_, imports)| RetainedAcceptedImports {
            checkpoint_target: imports.checkpoint_target.clone(),
            import_ordinals: imports.import_ordinals.clone(),
        })
        .collect::<Vec<_>>();
    let projected_imports = projections
        .iter()
        .filter(|projection| !projection.import_ordinals.is_empty())
        .map(|projection| ProjectedAcceptedImports {
            target: ScopedHistorySource {
                thread: projection.source_thread.clone(),
                source: projection.checkpoint_source.clone(),
            },
            import_ordinals: projection.import_ordinals.clone(),
        })
        .collect::<Vec<_>>();
    let retained = references
        .iter()
        .enumerate()
        .filter(|(ordinal, _)| !omitted.contains(ordinal))
        .collect::<Vec<_>>();
    // An accepted inherited turn can have been edited after this immutable
    // manifest was published. Its old revision is neither a current provider
    // receipt nor necessarily still materializable. Replace only that turn's
    // accepted whole-message group with its current canonical group. The
    // manifest still determines membership and order; later parent turns are
    // never discovered as members of this execution.
    let mut inherited_sources = BTreeMap::<String, BTreeSet<pioneer_compaction::SourceRef>>::new();
    let mut source_references =
        BTreeMap::<(String, pioneer_compaction::SourceRef), Vec<usize>>::new();
    for (ordinal, reference) in &retained {
        // The accepted manifest may have been captured while this was an OWN
        // message of its parent. Ownership is relative to the execution, not
        // to the source manifest's original capture.
        if reference.source_thread == execution_thread {
            continue;
        }
        for source in &reference.sources {
            inherited_sources
                .entry(reference.source_thread.clone())
                .or_default()
                .insert(source.clone());
            source_references
                .entry((reference.source_thread.clone(), source.clone()))
                .or_default()
                .push(*ordinal);
        }
    }
    let mut revised_turns = BTreeSet::<(String, String)>::new();
    for source in stale_direct_sources(store, workspace, inherited_sources).await? {
        for ordinal in source_references.get(&source).into_iter().flatten() {
            let reference = &references[*ordinal];
            let turn = accepted_reference_source_turn(reference)
                .ok_or_else(|| anyhow::anyhow!("changed accepted source has no single turn"))?;
            revised_turns.insert((reference.source_thread.clone(), turn));
        }
    }
    let mut revised_messages = BTreeMap::<(String, String), Vec<ChatMessage>>::new();
    for source_thread in revised_turns
        .iter()
        .map(|(thread, _)| thread.clone())
        .collect::<BTreeSet<_>>()
    {
        let selected_turns = revised_turns
            .iter()
            .filter(|(thread, _)| thread == &source_thread)
            .map(|(_, turn)| turn.clone())
            .collect::<BTreeSet<_>>();
        let fence = store.compaction_history_read_fence().await?;
        for mut message in super::history::load_task_line_history_turns(
            store,
            workspace,
            &source_thread,
            &selected_turns,
            &fence,
        )
        .await?
        {
            let Some(origin) = message.provenance.as_ref() else {
                continue;
            };
            let Some(turn) = accepted_provenance_source_turn(origin) else {
                continue;
            };
            let key = (source_thread.clone(), turn);
            if revised_turns.contains(&key) {
                // This is a parent-owned canonical turn, admitted as inherited
                // work by the immutable child manifest. Keep its real source
                // identity and new revision while expressing that execution
                // relationship to the checkpoint projector.
                if let Some(origin) = message.provenance.as_mut() {
                    origin.inherited = true;
                    origin.context_thread = Some(execution_thread.to_owned());
                }
                revised_messages.entry(key).or_default().push(message);
            }
        }
    }
    let mut revised_anchors = BTreeMap::<(String, String), usize>::new();
    for (ordinal, reference) in &retained {
        let Some(turn) = accepted_reference_source_turn(reference) else {
            continue;
        };
        let key = (reference.source_thread.clone(), turn);
        if revised_turns.contains(&key) {
            ensure!(
                reference.source_thread != execution_thread
                    && reference.complete
                    && !reference.protected_input,
                "changed accepted source cannot replace a partial or protected round"
            );
            ensure!(
                !accepted.contains_key(ordinal),
                "changed accepted source cannot replace an imported output grant"
            );
            revised_anchors.entry(key).or_insert(*ordinal);
        }
    }
    for (source_thread, turn) in &revised_turns {
        let (_, current) = store
            .get_turn(source_thread, turn)
            .await?
            .ok_or_else(|| anyhow::anyhow!("changed accepted source turn disappeared"))?;
        ensure!(
            current.message_deleted
                || revised_messages
                    .get(&(source_thread.clone(), turn.clone()))
                    .is_some_and(|messages| !messages.is_empty()),
            "changed accepted source has no complete current projection"
        );
    }
    let mut direct_sources = BTreeSet::new();
    direct_sources.extend(projections.iter().map(|projection| ScopedHistorySource {
        thread: projection.source_thread.clone(),
        source: projection.checkpoint_source.clone(),
    }));
    let mut messages = Vec::with_capacity(retained.len().saturating_add(projections.len()));
    let mut original_ordinals = Vec::with_capacity(retained.len());
    let proof_references = references
        .iter()
        .filter(|reference| {
            accepted_reference_source_turn(reference).is_none_or(|turn| {
                !revised_turns.contains(&(reference.source_thread.clone(), turn))
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut restore_state = FrozenExecutionRestoreState::from_references(
        store,
        workspace,
        allowed,
        &proof_references,
        checkpoint_graphs,
    )
    .await?;
    for projection in &projections {
        restore_state.extend_input_coverage(projection.coverage.iter());
        restore_state.extend_event_input_evidence(&projection.event_input_evidence);
    }
    restore_state.extend_input_coverage(externally_covered.iter());
    let retained = retained
        .into_iter()
        .filter(|(_, reference)| {
            accepted_reference_source_turn(reference).is_none_or(|turn| {
                !revised_turns.contains(&(reference.source_thread.clone(), turn))
            })
        })
        .collect::<Vec<_>>();
    for page in retained.chunks(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize) {
        let page_references = page
            .iter()
            .map(|(_, reference)| (*reference).clone())
            .collect::<Vec<_>>();
        let restored = restore_execution_entries_page(
            store,
            workspace,
            allowed,
            &page_references,
            checkpoint_graphs,
            &mut restore_state,
        )
        .await?;
        ensure!(
            restored.len() == page.len(),
            "frozen history count mismatch"
        );
        for ((ordinal, reference), message) in page.iter().zip(restored) {
            let Some(message) = message else {
                continue;
            };
            direct_sources.extend(reference.sources.iter().cloned().map(|source| {
                ScopedHistorySource {
                    thread: reference.source_thread.clone(),
                    source,
                }
            }));
            direct_sources.extend(reference.replay_source.iter().cloned().map(|source| {
                ScopedHistorySource {
                    thread: reference.source_thread.clone(),
                    source,
                }
            }));
            messages.push(message);
            original_ordinals.push(*ordinal);
        }
    }
    for (index, ordinal) in original_ordinals.iter().copied().enumerate() {
        if accepted.contains_key(&ordinal) {
            let origin = messages[index]
                .provenance
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("accepted own import has no canonical message"))?;
            origin.context_thread = Some(execution_thread.into());
            origin.inherited = false;
        }
    }
    let retained = original_ordinals
        .into_iter()
        .zip(messages)
        .chain(revised_anchors.into_iter().flat_map(|(key, anchor)| {
            revised_messages
                .remove(&key)
                .unwrap_or_default()
                .into_iter()
                .map(move |message| (anchor, message))
        }))
        .collect::<Vec<_>>();
    let mut direct_sources = direct_sources;
    for (_, message) in &retained {
        if let Some(origin) = message.provenance.as_ref() {
            direct_sources.extend(origin.sources.iter().cloned().map(|source| {
                ScopedHistorySource {
                    thread: origin.thread_id.clone(),
                    source: SourceRef {
                        scope: source.scope,
                        id: source.id,
                        version: source.version,
                    },
                }
            }));
        }
    }
    let mut replacements = Vec::with_capacity(projections.len());
    for projection in projections {
        let message = super::checkpoint::checkpoint_message_with_resolver(
            store,
            super::checkpoint::ProjectionContext {
                workspace,
                context_thread: execution_thread,
                source_thread: &projection.source_thread,
                owner: &projection.owner,
                allowed,
                allow_historical_gaps: true,
            },
            &projection.checkpoint,
            checkpoint_graphs,
        )
        .await?;
        replacements.push((projection.anchor, projection.checkpoint, message));
    }
    Ok(RestoredExecutionBasis {
        messages: order_execution_projection(retained, replacements),
        direct_sources: direct_sources.into_iter().collect(),
        retained_imports,
        projected_imports,
    })
}

fn order_execution_projection(
    retained: Vec<(usize, ChatMessage)>,
    replacements: Vec<(usize, String, ChatMessage)>,
) -> Vec<ChatMessage> {
    let mut ordered = retained
        .into_iter()
        .map(|(ordinal, message)| (ordinal, 1_u8, String::new(), message))
        .collect::<Vec<_>>();
    ordered.extend(
        replacements
            .into_iter()
            .map(|(anchor, checkpoint, message)| (anchor, 0_u8, checkpoint, message)),
    );
    ordered.sort_by(|left, right| (&left.0, &left.1, &left.2).cmp(&(&right.0, &right.1, &right.2)));
    ordered
        .into_iter()
        .map(|(_, _, _, message)| message)
        .collect()
}

/// Prepare an already accepted execution snapshot without changing literal
/// frozen-history restoration for verification and concurrent-winner callers.
/// If the manifest belongs to the accepted parent or the execution itself,
/// compatible published checkpoints are selected from immutable metadata
/// before raw payload reads.
pub(crate) struct RestoredAcceptedHistory {
    pub(crate) messages: Vec<ChatMessage>,
    pub(crate) manifest_owner: Option<String>,
    /// Exact canonical sources visible to this execution projection. This is
    /// deliberately distinct from the immutable accepted boundary: a later
    /// compatible checkpoint can replace covered raw entries without changing
    /// the TaskRun/runtime snapshot that granted access to them.
    pub(crate) direct_sources: Vec<ScopedHistorySource>,
}

pub(crate) async fn restore_accepted_history_for_execution(
    store: &CrudStore,
    workspace: &str,
    parent: Option<&str>,
    execution_thread: &str,
    allowed: &BTreeSet<String>,
    history_json: &str,
) -> Result<RestoredAcceptedHistory> {
    if history_json.trim_start().starts_with('[') {
        return Ok(RestoredAcceptedHistory {
            messages: serde_json::from_str(history_json)
                .context("invalid legacy conversation history")?,
            // Legacy arrays have no immutable direct-reference proof and are
            // therefore never eligible for a provider continuity receipt.
            direct_sources: Vec::new(),
            manifest_owner: None,
        });
    }
    let descriptor: FrozenHistoryRef = serde_json::from_str(history_json)?;
    let owner = store
        .compaction_frozen_history_owner(workspace, &descriptor)
        .await?;
    if let Some(owner) = owner.as_deref()
        && (owner == execution_thread || parent == Some(owner))
    {
        let mut accepted = allowed.clone();
        accepted.insert(execution_thread.to_owned());
        let mut checkpoint_graphs = super::coverage::CheckpointGraphResolver::default();
        let restored = restore_accepted_execution_basis_prepared(
            store,
            workspace,
            owner,
            execution_thread,
            &accepted,
            &descriptor,
            &BTreeSet::new(),
            &mut checkpoint_graphs,
        )
        .await?;
        return Ok(RestoredAcceptedHistory {
            messages: restored.messages,
            direct_sources: restored.direct_sources,
            manifest_owner: Some(owner.to_owned()),
        });
    }
    let (messages, direct_sources) = if let Some(owner) = owner.as_deref() {
        restore_accepted_execution_projection_without_checkpoint(
            store,
            workspace,
            owner,
            allowed,
            &descriptor,
            None,
        )
        .await?
    } else {
        // Preserve the established missing-manifest diagnostic. There is no
        // owner to substitute with the execution thread or to authorize a
        // typed execution projection.
        let messages = restore(store, workspace, allowed, &descriptor).await?;
        (messages, Vec::new())
    };
    Ok(RestoredAcceptedHistory {
        messages,
        direct_sources,
        manifest_owner: owner,
    })
}

/// Project the exact snapshot won by insert-if-absent for execution. The
/// caller verifies its literal wire first; this path does not discover a newer
/// checkpoint or reuse messages from the losing capture.
pub(crate) async fn restore_accepted_snapshot_for_execution_without_checkpoint(
    store: &CrudStore,
    workspace: &str,
    parent: &str,
    execution_thread: &str,
    history_json: &str,
) -> Result<Vec<ChatMessage>> {
    if history_json.trim_start().starts_with('[') {
        return serde_json::from_str(history_json).context("invalid legacy conversation history");
    }
    let descriptor: FrozenHistoryRef = serde_json::from_str(history_json)?;
    let mut allowed = accepted_history_scopes(store, workspace, parent, history_json).await?;
    allowed.insert(execution_thread.to_owned());
    let (messages, _) = restore_accepted_execution_projection_without_checkpoint(
        store,
        workspace,
        parent,
        &allowed,
        &descriptor,
        Some(execution_thread),
    )
    .await?;
    Ok(messages)
}

async fn restore_accepted_execution_projection_without_checkpoint(
    store: &CrudStore,
    workspace: &str,
    owner: &str,
    allowed: &BTreeSet<String>,
    descriptor: &FrozenHistoryRef,
    execution_thread: Option<&str>,
) -> Result<(Vec<ChatMessage>, Vec<ScopedHistorySource>)> {
    let (_, references) =
        frozen_manifest_references(store, workspace, Some(owner), descriptor, Some(allowed))
            .await?;
    let mut checkpoint_graphs = super::coverage::CheckpointGraphResolver::default();
    let accepted = read_accepted_imports(
        store,
        workspace,
        owner,
        descriptor,
        &references,
        execution_thread,
        &mut checkpoint_graphs,
    )
    .await?;
    let mut state = FrozenExecutionRestoreState::from_references(
        store,
        workspace,
        allowed,
        &references,
        &mut checkpoint_graphs,
    )
    .await?;
    let mut messages = Vec::with_capacity(references.len());
    let mut direct_sources = BTreeSet::new();
    for (page_index, page) in references
        .chunks(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize)
        .enumerate()
    {
        let restored = restore_execution_entries_page(
            store,
            workspace,
            allowed,
            page,
            &mut checkpoint_graphs,
            &mut state,
        )
        .await?;
        ensure!(
            restored.len() == page.len(),
            "frozen history count mismatch"
        );
        let page_start = page_index * pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize;
        for (page_ordinal, (reference, message)) in page.iter().zip(restored).enumerate() {
            let Some(mut message) = message else {
                continue;
            };
            if let Some(execution_thread) = execution_thread {
                hydrate_visible_messages(
                    std::slice::from_mut(&mut message),
                    &references,
                    &[page_start + page_ordinal],
                    &accepted,
                    execution_thread,
                )?;
            }
            direct_sources.extend(reference.sources.iter().cloned().map(|source| {
                ScopedHistorySource {
                    thread: reference.source_thread.clone(),
                    source,
                }
            }));
            direct_sources.extend(reference.replay_source.iter().cloned().map(|source| {
                ScopedHistorySource {
                    thread: reference.source_thread.clone(),
                    source,
                }
            }));
            messages.push(message);
        }
    }
    Ok((messages, direct_sources.into_iter().collect()))
}

pub(crate) async fn frozen_history_direct_sources(
    store: &CrudStore,
    workspace: &str,
    descriptor: &FrozenHistoryRef,
) -> Result<Vec<ScopedHistorySource>> {
    frozen_history_projection_sources(store, workspace, descriptor, None).await
}

/// Keep the immutable manifest as authority, but guard the representation
/// actually sent: published summaries replace covered raw source guards.
pub(crate) async fn frozen_history_projection_sources(
    store: &CrudStore,
    workspace: &str,
    descriptor: &FrozenHistoryRef,
    messages: Option<&[ChatMessage]>,
) -> Result<Vec<ScopedHistorySource>> {
    let projected = messages.map(|messages| {
        messages
            .iter()
            .flat_map(|message| {
                message.provenance.iter().flat_map(|origin| {
                    origin.sources.iter().map(|reference| ScopedHistorySource {
                        thread: origin.thread_id.clone(),
                        source: source(reference),
                    })
                })
            })
            .collect::<BTreeSet<_>>()
    });
    let (_, references) =
        frozen_manifest_references(store, workspace, None, descriptor, None).await?;
    let mut sources = projected.clone().unwrap_or_default();
    for reference in references {
        if projected.as_ref().is_some_and(|sources| {
            !reference.sources.iter().all(|source| {
                sources.contains(&ScopedHistorySource {
                    thread: reference.source_thread.clone(),
                    source: source.clone(),
                })
            })
        }) {
            continue;
        }
        sources.extend(
            reference
                .sources
                .into_iter()
                .map(|source| ScopedHistorySource {
                    thread: reference.source_thread.clone(),
                    source,
                }),
        );
        sources.extend(
            reference
                .replay_source
                .into_iter()
                .map(|source| ScopedHistorySource {
                    thread: reference.source_thread.clone(),
                    source,
                }),
        );
    }
    Ok(sources.into_iter().collect())
}

async fn restore_with_resolver(
    store: &CrudStore,
    workspace: &str,
    allowed_threads: &BTreeSet<String>,
    descriptor: &FrozenHistoryRef,
    checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
) -> Result<Vec<ChatMessage>> {
    #[cfg(test)]
    if let Some(state) = STORE_RESTORE_CALLS
        .lock()
        .unwrap()
        .get(&(
            store.database_connection().runtime_identity(),
            workspace.to_owned(),
        ))
        .and_then(std::sync::Weak::upgrade)
    {
        state.descriptors.lock().unwrap().push(descriptor.clone());
    }
    let (owner, references) =
        frozen_manifest_references(store, workspace, None, descriptor, Some(allowed_threads))
            .await?;
    let mut restored = Vec::with_capacity(references.len());
    for page in references.chunks(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize) {
        restored.extend(
            restore_entries_page(store, workspace, allowed_threads, &page, checkpoint_graphs)
                .await?,
        );
    }
    ensure!(
        restored.len() == references.len(),
        "frozen history count mismatch"
    );
    let _ = read_accepted_imports(
        &store,
        workspace,
        &owner,
        descriptor,
        &references,
        None,
        checkpoint_graphs,
    )
    .await?;
    Ok(restored)
}

async fn restore_model_with_resolver(
    store: &CrudStore,
    workspace: &str,
    allowed_threads: &BTreeSet<String>,
    descriptor: &FrozenHistoryRef,
    checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
) -> Result<Vec<ChatMessage>> {
    let (owner, references) =
        frozen_manifest_references(store, workspace, None, descriptor, Some(allowed_threads))
            .await?;
    let _ = read_accepted_imports(
        store,
        workspace,
        &owner,
        descriptor,
        &references,
        None,
        checkpoint_graphs,
    )
    .await?;
    let mut state = FrozenExecutionRestoreState::from_references(
        store,
        workspace,
        allowed_threads,
        &references,
        checkpoint_graphs,
    )
    .await?;
    let mut messages = Vec::with_capacity(references.len());
    for page in references.chunks(pioneer_crud::compaction::SOURCE_PAGE_ROWS as usize) {
        messages.extend(
            restore_execution_entries_page(
                store,
                workspace,
                allowed_threads,
                page,
                checkpoint_graphs,
                &mut state,
            )
            .await?
            .into_iter()
            .flatten(),
        );
    }
    Ok(messages)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FrozenRestoreProjection {
    Literal,
    Execution,
}

#[derive(Default)]
struct FrozenExecutionRestoreState {
    authoritative_inputs: BTreeSet<(String, String)>,
}

impl FrozenExecutionRestoreState {
    async fn from_references(
        store: &CrudStore,
        workspace: &str,
        allowed: &BTreeSet<String>,
        references: &[FrozenMessageRef],
        checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
    ) -> Result<Self> {
        let mut state = Self::default();
        let mut event_sources = BTreeMap::<String, BTreeSet<SourceRef>>::new();
        let mut checkpoint_sources = BTreeSet::new();
        for reference in references {
            if reference.event_input_role
                == Some(pioneer_compaction::frozen::FrozenEventInputRole::Authoritative)
                && let Some(turn) = reference.sources.first().and_then(source_turn)
            {
                state
                    .authoritative_inputs
                    .insert((reference.source_thread.clone(), turn.to_owned()));
            }
            for source in &reference.sources {
                if source.scope.starts_with("input:")
                    && let Some(turn) = source_turn(source)
                {
                    state
                        .authoritative_inputs
                        .insert((reference.source_thread.clone(), turn.to_owned()));
                }
                if source.scope.starts_with("event:") && reference.event_input_role.is_none() {
                    event_sources
                        .entry(reference.source_thread.clone())
                        .or_default()
                        .insert(source.clone());
                }
                if source.scope.starts_with("checkpoint:") {
                    checkpoint_sources.insert(source.clone());
                }
            }
        }
        for source in checkpoint_sources {
            let graph = checkpoint_graphs
                .resolve(store, workspace, Some(allowed), &source)
                .await?
                .ok_or_else(|| anyhow::anyhow!("checkpoint root is unavailable"))?;
            state.extend_input_coverage(graph.leaves.iter());
            state.extend_event_input_evidence(&graph.event_input_evidence);
        }
        // Resolve event-input identity from the captured exact revisions before
        // checkpoint replacement removes covered entries. This reads only
        // retained exact projection metadata: no covered payload or raw liveness is
        // consulted, and an arbitrary event-scoped source is never promoted to
        // authoritative input.
        for (thread, sources) in event_sources {
            for projection in
                super::history::historical_event_projections(store, workspace, &thread, sources)
                    .await?
            {
                if matches!(
                    projection.projection_kind.as_str(),
                    "input" | "input_revision"
                ) && let Some(turn) = source_turn(&projection.reference)
                {
                    state
                        .authoritative_inputs
                        .insert((thread.clone(), turn.to_owned()));
                }
            }
        }
        Ok(state)
    }

    fn extend_input_coverage<'a>(
        &mut self,
        sources: impl IntoIterator<Item = &'a ScopedHistorySource>,
    ) {
        self.authoritative_inputs
            .extend(sources.into_iter().filter_map(|source| {
                source
                    .source
                    .scope
                    .strip_prefix("input:")
                    .map(|turn| (source.thread.clone(), turn.to_owned()))
            }));
    }

    fn extend_event_input_evidence(&mut self, evidence: &BTreeMap<ScopedHistorySource, String>) {
        self.authoritative_inputs.extend(
            evidence
                .iter()
                .filter(|(_, role)| role.as_str() == "authoritative")
                .filter_map(|(source, _)| {
                    source_turn(&source.source).map(|turn| (source.thread.clone(), turn.to_owned()))
                }),
        );
    }
}

fn source_turn(source: &SourceRef) -> Option<&str> {
    source.scope.split_once(':').map(|(_, turn)| turn)
}

async fn restore_entry(
    store: &CrudStore,
    workspace: &str,
    _allowed_threads: &BTreeSet<String>,
    reference: &FrozenMessageRef,
    _checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
    projection: FrozenRestoreProjection,
    state: &mut FrozenExecutionRestoreState,
) -> Result<Option<ChatMessage>> {
    reference.validate()?;
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
        let execution = super::history::input_message(&inputs)?;
        let candidates = vec![
            FrozenModelCandidate::exact(execution.clone()),
            FrozenModelCandidate::exact(super::history::legacy_input_message(&inputs)?),
        ];
        let restored = finish_restored_entry(
            store,
            workspace,
            reference,
            candidates,
            projection,
            Some(Some(execution)),
        )
        .await?;
        if projection == FrozenRestoreProjection::Execution {
            for source in &reference.sources {
                if let Some(turn) = source_turn(source) {
                    state
                        .authoritative_inputs
                        .insert((reference.source_thread.clone(), turn.to_owned()));
                }
            }
        }
        return Ok(restored);
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
    restore_entry_from_payloads(
        store,
        workspace,
        reference,
        std::slice::from_ref(&payload),
        projection,
        state,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn restore_reference_for_test(
    store: &CrudStore,
    workspace: &str,
    reference: &FrozenMessageRef,
) -> Result<ChatMessage> {
    restore_entry(
        store,
        workspace,
        &BTreeSet::from([reference.source_thread.clone()]),
        reference,
        &mut super::coverage::CheckpointGraphResolver::default(),
        FrozenRestoreProjection::Execution,
        &mut FrozenExecutionRestoreState::default(),
    )
    .await?
    .ok_or_else(|| anyhow::anyhow!("test source has no model projection"))
}

/// Restore a bounded manifest page. Consecutive single-source input/context
/// entries share the CRUD byte/row bounded read; each message is projected and
/// wire-checked only after that reader has been released.
async fn restore_entries_page(
    store: &CrudStore,
    workspace: &str,
    allowed_threads: &BTreeSet<String>,
    references: &[FrozenMessageRef],
    checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
) -> Result<Vec<ChatMessage>> {
    let mut state = FrozenExecutionRestoreState::default();
    let restored = restore_entries_page_with_projection(
        store,
        workspace,
        allowed_threads,
        references,
        checkpoint_graphs,
        FrozenRestoreProjection::Literal,
        &mut state,
    )
    .await?;
    restored
        .into_iter()
        .map(|message| {
            message.ok_or_else(|| anyhow::anyhow!("literal frozen restore omitted a message"))
        })
        .collect()
}

async fn restore_execution_entries_page(
    store: &CrudStore,
    workspace: &str,
    allowed_threads: &BTreeSet<String>,
    references: &[FrozenMessageRef],
    checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
    state: &mut FrozenExecutionRestoreState,
) -> Result<Vec<Option<ChatMessage>>> {
    restore_entries_page_with_projection(
        store,
        workspace,
        allowed_threads,
        references,
        checkpoint_graphs,
        FrozenRestoreProjection::Execution,
        state,
    )
    .await
}

async fn restore_entries_page_with_projection(
    store: &CrudStore,
    workspace: &str,
    allowed_threads: &BTreeSet<String>,
    references: &[FrozenMessageRef],
    checkpoint_graphs: &mut super::coverage::CheckpointGraphResolver,
    projection: FrozenRestoreProjection,
    state: &mut FrozenExecutionRestoreState,
) -> Result<Vec<Option<ChatMessage>>> {
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
            result.push(
                restore_entry(
                    store,
                    workspace,
                    allowed_threads,
                    reference,
                    checkpoint_graphs,
                    projection,
                    state,
                )
                .await?,
            );
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
                    projection,
                    state,
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
    projection: FrozenRestoreProjection,
    state: &mut FrozenExecutionRestoreState,
) -> Result<Option<ChatMessage>> {
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
        let execution = super::history::input_message(&inputs)?;
        candidates.push(FrozenModelCandidate::exact(execution.clone()));
        candidates.push(FrozenModelCandidate::exact(
            super::history::legacy_input_message(&inputs)?,
        ));
        let restored = finish_restored_entry(
            store,
            workspace,
            reference,
            candidates,
            projection,
            Some(Some(execution)),
        )
        .await?;
        if projection == FrozenRestoreProjection::Execution {
            for source in &reference.sources {
                if let Some(turn) = source_turn(source) {
                    state
                        .authoritative_inputs
                        .insert((reference.source_thread.clone(), turn.to_owned()));
                }
            }
        }
        return Ok(restored);
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
                candidates.push(FrozenModelCandidate::exact(ChatMessage::user(format!(
                    "Historical turn failed: {:?}",
                    value.turn.error
                ))));
            }
            if let Some(message) = super::history::legacy_event_message(event.clone())? {
                candidates.push(FrozenModelCandidate::exact(message));
            }
            if let Some(message) = super::history::event_message(event.clone())? {
                candidates.push(FrozenModelCandidate::exact(message));
            }
            if let Some(message) =
                super::history::event_message_suppressing_input_copy_media(event.clone())?
            {
                candidates.push(FrozenModelCandidate::exact(message));
            }
            let event_turn = event.turn_id().to_owned();
            let authoritative_input = matches!(
                &event,
                pioneer_crud::CanonicalTurnEventPayload::TurnStarted(value)
                    if !value.input.is_empty()
            ) || matches!(
                &event,
                pioneer_crud::CanonicalTurnEventPayload::TurnMessageEdited(value)
                    if !value.input.is_empty()
            );
            let input_copy = matches!(
                &event,
                pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(value)
                    if matches!(&value.item, pioneer_protocol::TurnItem::UserMessage { .. })
            );
            let suppress_copy = input_copy
                && state
                    .authoritative_inputs
                    .contains(&(reference.source_thread.clone(), event_turn.clone()));
            let execution = if suppress_copy {
                super::history::event_message_suppressing_input_copy_media(event)?
            } else {
                super::history::event_message(event)?
            };
            let restored = finish_restored_entry(
                store,
                workspace,
                reference,
                candidates,
                projection,
                Some(execution),
            )
            .await?;
            if projection == FrozenRestoreProjection::Execution && authoritative_input {
                state
                    .authoritative_inputs
                    .insert((reference.source_thread.clone(), event_turn));
            }
            return Ok(restored);
        } else if source.scope.starts_with("task-basis:") {
            candidates.extend(
                serde_json::from_str::<Vec<ChatMessage>>(payload)?
                    .into_iter()
                    .map(FrozenModelCandidate::exact),
            );
        } else if source.scope.starts_with("checkpoint:") {
            candidates.push(FrozenModelCandidate::exact(ChatMessage::user(format!(
                "Summary of completed work (historical data):\n{payload}"
            ))));
        } else if source.scope.starts_with("context:") {
            if let Ok(envelope) = serde_json::from_str::<CanonicalProviderRoundEnvelope>(payload) {
                if envelope.termination == pioneer_provider::ProviderTermination::ProviderError {
                    candidates.push(FrozenModelCandidate::exact(
                        super::history::provider_observation(payload)?,
                    ));
                } else {
                    candidates.push(FrozenModelCandidate::exact(envelope.message));
                }
            } else if let Ok(view) = serde_json::from_str::<pioneer_tools::ToolResultView>(payload)
            {
                if let pioneer_tools::ToolResultView::Json {
                    value,
                    truncated: false,
                } = view
                {
                    let message: ChatMessage = serde_json::from_value(value)?;
                    candidates.push(FrozenModelCandidate::exact(message));
                }
            }
            candidates.push(FrozenModelCandidate::exact(ChatMessage::user(format!(
                "Legacy provider observation (available original):\n{payload}"
            ))));
            candidates.push(FrozenModelCandidate::exact(ChatMessage::user(format!(
                "Legacy provider observation; outcome is not inferred:\n{payload}"
            ))));
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
                    candidates.push(FrozenModelCandidate::exact(serde_json::from_value(value)?));
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
                candidates.push(FrozenModelCandidate::exact(message));
            }
        }
    }
    finish_restored_entry(store, workspace, reference, candidates, projection, None).await
}

async fn finish_restored_entry(
    store: &CrudStore,
    workspace: &str,
    reference: &FrozenMessageRef,
    mut candidates: Vec<FrozenModelCandidate>,
    projection: FrozenRestoreProjection,
    execution: Option<Option<ChatMessage>>,
) -> Result<Option<ChatMessage>> {
    let replay_source = reference
        .replay_source
        .as_ref()
        .unwrap_or(&reference.sources[0]);
    if candidates
        .iter()
        .any(|candidate| candidate.wire.role == pioneer_provider::Role::Tool)
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
        for candidate in full
            .into_iter()
            .filter(|candidate| candidate.wire.role == pioneer_provider::Role::Tool)
        {
            let wire =
                pioneer_agent::compaction::restored_tool_result_message(&candidate.wire, &locator)?;
            let model = pioneer_agent::compaction::restored_tool_result_message(
                &candidate.model,
                &locator,
            )?;
            candidates.push(FrozenModelCandidate::upgraded(wire, model));
        }
    }
    // The wire hash chooses an exact deterministic projection of a known source,
    // never a similar text or an inferred coverage boundary.
    let base = candidates.clone();
    for candidate in base {
        let interrupted = |message: &ChatMessage| -> Result<ChatMessage> {
            Ok(ChatMessage::user(format!(
                "Interrupted canonical round; some tool outcomes are unknown. Historical observation, not a new call:\n{}",
                serde_json::to_string(message)?
            )))
        };
        candidates.push(FrozenModelCandidate::upgraded(
            interrupted(&candidate.wire)?,
            interrupted(&candidate.model)?,
        ));
    }
    let verified = verified_model_candidate(&reference.wire_sha256, candidates)?;
    let mut message = match projection {
        FrozenRestoreProjection::Literal => Some(verified.wire),
        FrozenRestoreProjection::Execution => execution.unwrap_or(Some(verified.model)),
    };
    let Some(projected) = message.as_mut() else {
        return Ok(None);
    };
    projected.provenance = Some(MessageProvenance {
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
    fn legacy_frozen_command_hash_is_verified_before_model_upgrade() {
        let item = pioneer_protocol::TurnItem::CommandExecution {
            id: "command".into(),
            tool_name: "exec_command".into(),
            arguments: serde_json::json!({"command":["true"]}),
            status: pioneer_protocol::ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(
                "exec_command",
            ),
            display: pioneer_protocol::ToolDisplayPayload::Shell {
                stdout: Some("frozen-unique-output".into()),
                stderr: None,
                aggregated_output: Some("frozen-unique-output".into()),
                exit_code: Some(0),
                duration_ms: None,
                timed_out: Some(false),
                truncated: false,
            },
            storage: pioneer_protocol::ToolStoragePayload::Shell {
                stdout: Some("frozen-unique-output".into()),
                stderr: None,
                aggregated_output: Some("frozen-unique-output".into()),
                exit_code: Some(0),
                duration_ms: None,
                timed_out: Some(false),
                truncated: false,
            },
            recovery: None,
            command: vec!["true".into()],
            cwd: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        let legacy = ChatMessage::user(format!(
            "Recorded historical event:\n{}",
            serde_json::to_string(&item).unwrap()
        ));
        let current =
            super::history::event_message(pioneer_crud::CanonicalTurnEventPayload::ItemCompleted(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: "ws".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item: item.clone(),
                },
            ))
            .unwrap()
            .unwrap();
        let hash = wire_digest(&legacy).unwrap();
        let projected = verified_model_message(
            &hash,
            vec![FrozenModelCandidate::upgraded(legacy, current.clone())],
        )
        .unwrap();
        assert_eq!(projected.content.matches("frozen-unique-output").count(), 1);
        assert!(
            verified_model_message(&"0".repeat(64), vec![FrozenModelCandidate::exact(current)])
                .is_err()
        );
    }

    #[test]
    fn verified_non_event_text_is_never_upgraded_by_prefix() {
        let item = pioneer_protocol::TurnItem::CommandExecution {
            id: "pasted-command".into(),
            tool_name: "exec_command".into(),
            arguments: serde_json::json!({}),
            status: pioneer_protocol::ToolCallStatus::Completed,
            recovery_policy: None,
            output_policy: pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name(
                "exec_command",
            ),
            display: pioneer_protocol::ToolDisplayPayload::Hidden,
            storage: pioneer_protocol::ToolStoragePayload::None,
            recovery: None,
            command: vec!["true".into()],
            cwd: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        let pasted = ChatMessage::user(format!(
            "Recorded historical event:\n{}",
            serde_json::to_string(&item).unwrap()
        ));
        let hash = wire_digest(&pasted).unwrap();
        assert_eq!(
            verified_model_message(&hash, vec![FrozenModelCandidate::exact(pasted.clone())])
                .unwrap(),
            pasted
        );

        let assistant = ChatMessage::assistant(format!(
            "Recorded historical event:\n{}",
            serde_json::to_string(&item).unwrap()
        ));
        let hash = wire_digest(&assistant).unwrap();
        assert_eq!(
            verified_model_message(&hash, vec![FrozenModelCandidate::exact(assistant.clone())])
                .unwrap(),
            assistant
        );
    }

    #[test]
    fn hydration_uses_original_ordinals_with_duplicate_provenance_and_filtered_gaps() {
        let source = SourceRef {
            scope: "task-basis:legacy-run".into(),
            id: "legacy-run".into(),
            version: "task-basis-revision:1".into(),
        };
        let reference = FrozenMessageRef {
            logical_turn_id: None,
            source_thread: "parent".into(),
            context_thread: None,
            unit_id: "legacy-task-basis:legacy-run".into(),
            sources: vec![source.clone()],
            inherited: true,
            complete: true,
            protected_input: false,
            wire_sha256: "a".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
            event_input_role: None,
        };
        let mut duplicate = ChatMessage::assistant("same legacy message");
        duplicate.provenance = Some(MessageProvenance {
            logical_turn_id: None,
            workspace_id: "ws".into(),
            thread_id: "parent".into(),
            context_thread: None,
            unit_id: "legacy-task-basis:legacy-run".into(),
            sources: vec![MessageSourceRef {
                scope: source.scope,
                id: source.id,
                version: source.version,
            }],
            inherited: true,
            complete: true,
            protected_input: false,
        });
        let mut messages = vec![duplicate.clone(), duplicate];
        let references = vec![reference.clone(), reference.clone(), reference.clone()];
        let accepted = BTreeMap::from([
            (1, AcceptedMessageImports::default()),
            (2, AcceptedMessageImports::default()),
        ]);

        hydrate_visible_messages(
            &mut messages,
            &references,
            &[0, 2],
            &accepted,
            "child-execution",
        )
        .unwrap();

        let first = messages[0].provenance.as_ref().unwrap();
        assert_eq!(first.context_thread, None);
        assert!(first.inherited);
        let second = messages[1].provenance.as_ref().unwrap();
        assert_eq!(second.context_thread.as_deref(), Some("child-execution"));
        assert!(!second.inherited);
    }

    #[test]
    fn literal_hydration_keeps_hidden_ordinal_and_is_idempotent() {
        let source = SourceRef {
            scope: "task-basis:legacy-run".into(),
            id: "legacy-run".into(),
            version: "task-basis-revision:1".into(),
        };
        let reference = FrozenMessageRef {
            logical_turn_id: None,
            source_thread: "parent".into(),
            context_thread: None,
            unit_id: "legacy-task-basis:legacy-run".into(),
            sources: vec![source.clone()],
            inherited: true,
            complete: true,
            protected_input: false,
            wire_sha256: "a".repeat(64),
            replay_source: None,
            tool_item_id: None,
            tool_call_id: None,
            tool_name: None,
            event_input_role: None,
        };
        let message = |text: &str| {
            let mut message = ChatMessage::assistant(text);
            message.provenance = Some(MessageProvenance {
                logical_turn_id: None,
                workspace_id: "ws".into(),
                thread_id: "parent".into(),
                context_thread: None,
                unit_id: reference.unit_id.clone(),
                sources: vec![MessageSourceRef {
                    scope: source.scope.clone(),
                    id: source.id.clone(),
                    version: source.version.clone(),
                }],
                inherited: true,
                complete: true,
                protected_input: false,
            });
            message
        };
        let references = vec![reference.clone(), reference.clone(), reference.clone()];
        let accepted = BTreeMap::from([(2, AcceptedMessageImports::default())]);
        let mut literal = vec![message("A"), message("technical T"), message("B")];
        let ordinals = [0, 1, 2];
        hydrate_visible_messages(&mut literal, &references, &ordinals, &accepted, "execution")
            .unwrap();
        assert_eq!(
            literal
                .iter()
                .map(|m| m.content.as_str())
                .collect::<Vec<_>>(),
            ["A", "technical T", "B"]
        );
        assert!(literal[1].provenance.as_ref().unwrap().inherited);
        assert_eq!(
            literal[2]
                .provenance
                .as_ref()
                .unwrap()
                .context_thread
                .as_deref(),
            Some("execution")
        );
        let once = literal.clone();
        hydrate_visible_messages(&mut literal, &references, &ordinals, &accepted, "execution")
            .unwrap();
        assert_eq!(literal, once);
    }

    fn ordered_message(text: &str, thread: &str, source: &str) -> ChatMessage {
        let mut message = ChatMessage::user(text);
        message.provenance = Some(MessageProvenance {
            logical_turn_id: None,
            workspace_id: "ws".into(),
            thread_id: thread.into(),
            context_thread: Some("execution".into()),
            unit_id: source.into(),
            sources: vec![MessageSourceRef {
                scope: "event:turn".into(),
                id: source.into(),
                version: "event-revision:1".into(),
            }],
            complete: true,
            protected_input: false,
            inherited: false,
        });
        message
    }

    #[test]
    fn execution_projection_keeps_checkpoint_anchors_and_retained_provenance() {
        let before = ordered_message("before", "z-thread", "before");
        let middle = ordered_message("middle", "a-thread", "middle");
        let after = ordered_message("after", "z-thread", "after");
        let first = ordered_message("first summary", "z-thread", "checkpoint-z");
        let second = ordered_message("second summary", "a-thread", "checkpoint-a");
        let ordered = order_execution_projection(
            vec![(0, before.clone()), (3, middle.clone()), (6, after.clone())],
            vec![
                (1, "z-checkpoint".into(), first),
                (4, "a-checkpoint".into(), second),
            ],
        );
        assert_eq!(
            ordered
                .iter()
                .map(|message| message.content.as_str())
                .collect::<Vec<_>>(),
            vec![
                "before",
                "first summary",
                "middle",
                "second summary",
                "after"
            ]
        );
        assert_eq!(ordered[0].provenance, before.provenance);
        assert_eq!(ordered[2].provenance, middle.provenance);
        assert_eq!(ordered[4].provenance, after.provenance);
    }

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
