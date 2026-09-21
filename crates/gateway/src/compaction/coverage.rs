//! Read bounded historical checkpoint coverage. Published roots are validated
//! as objects; their saved leaves describe boundaries and are never revalidated
//! against today's canonical rows.
use super::*;
use pioneer_agent::compaction::composition::ScopedHistorySource;
use pioneer_crud::compaction::{CheckpointBody, CheckpointEdges, CheckpointMetadata};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

const DEFAULT_GRAPH_CACHE_UNITS: usize = 4 * 1024;
const DEFAULT_PAYLOAD_CACHE_BYTES: usize = 256 * 1024;
const MAX_HISTORICAL_GRAPH_NODES: usize = 65_536;

#[cfg(test)]
static NEXT_RESOLVER_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

#[cfg(test)]
#[derive(Default)]
struct PreparationWork {
    edge_loads: std::sync::atomic::AtomicUsize,
    body_loads: std::sync::atomic::AtomicUsize,
    closure_builds: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
static PREPARATION_WORK: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<(usize, String), std::sync::Weak<PreparationWork>>>,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
#[derive(Default)]
struct BodyLoadPauseState {
    reached: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
    reached_notify: tokio::sync::Notify,
    release_notify: tokio::sync::Notify,
}

#[cfg(test)]
static BODY_LOAD_PAUSES: std::sync::LazyLock<
    std::sync::Mutex<
        std::collections::HashMap<(usize, String, String), std::sync::Weak<BodyLoadPauseState>>,
    >,
> = std::sync::LazyLock::new(Default::default);

#[cfg(test)]
pub(crate) struct BodyLoadPause {
    key: (usize, String, String),
    state: Arc<BodyLoadPauseState>,
}

#[cfg(test)]
impl BodyLoadPause {
    pub(crate) async fn reached(&self) {
        loop {
            let notified = self.state.reached_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.reached.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn release(&self) {
        self.state
            .released
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.state.release_notify.notify_waiters();
    }
}

#[cfg(test)]
impl Drop for BodyLoadPause {
    fn drop(&mut self) {
        self.release();
        BODY_LOAD_PAUSES.lock().unwrap().remove(&self.key);
    }
}

#[cfg(test)]
pub(crate) fn pause_body_load(
    store: &CrudStore,
    workspace: &str,
    checkpoint: &str,
) -> BodyLoadPause {
    let key = (
        store.database_connection().runtime_identity(),
        workspace.to_owned(),
        checkpoint.to_owned(),
    );
    let state = Arc::new(BodyLoadPauseState::default());
    let previous = BODY_LOAD_PAUSES
        .lock()
        .unwrap()
        .insert(key.clone(), Arc::downgrade(&state));
    assert!(
        previous.is_none(),
        "checkpoint body pause already installed"
    );
    BodyLoadPause { key, state }
}

#[cfg(test)]
pub(crate) struct PreparationWorkObserver {
    key: (usize, String),
    work: Arc<PreparationWork>,
}

#[cfg(test)]
impl PreparationWorkObserver {
    pub(crate) fn edge_loads(&self) -> usize {
        self.work
            .edge_loads
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn body_loads(&self) -> usize {
        self.work
            .body_loads
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn closure_builds(&self) -> usize {
        self.work
            .closure_builds
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparationWorkSnapshot {
    pub(crate) edge_loads: usize,
    pub(crate) body_loads: usize,
    pub(crate) closure_builds: usize,
}

#[cfg(test)]
pub(crate) fn preparation_work_snapshot(
    store: &CrudStore,
    workspace: &str,
) -> Option<PreparationWorkSnapshot> {
    PREPARATION_WORK
        .lock()
        .unwrap()
        .get(&(
            store.database_connection().runtime_identity(),
            workspace.to_owned(),
        ))
        .and_then(std::sync::Weak::upgrade)
        .map(|work| PreparationWorkSnapshot {
            edge_loads: work.edge_loads.load(std::sync::atomic::Ordering::SeqCst),
            body_loads: work.body_loads.load(std::sync::atomic::Ordering::SeqCst),
            closure_builds: work
                .closure_builds
                .load(std::sync::atomic::Ordering::SeqCst),
        })
}

#[cfg(test)]
impl Drop for PreparationWorkObserver {
    fn drop(&mut self) {
        PREPARATION_WORK.lock().unwrap().remove(&self.key);
    }
}

#[cfg(test)]
pub(crate) fn observe_preparation_work(
    store: &CrudStore,
    workspace: &str,
) -> PreparationWorkObserver {
    let key = (
        store.database_connection().runtime_identity(),
        workspace.to_owned(),
    );
    let work = Arc::new(PreparationWork::default());
    let previous = PREPARATION_WORK
        .lock()
        .unwrap()
        .insert(key.clone(), Arc::downgrade(&work));
    assert!(
        previous.is_none(),
        "preparation work observer already installed"
    );
    PreparationWorkObserver { key, work }
}

pub(crate) struct ResolvedCheckpointGraph {
    pub(crate) leaves: BTreeSet<ScopedHistorySource>,
    /// Saved transport aliases for covered tool results. They participate in
    /// projection/filtering only, never in coverage or access grants.
    pub(crate) replay_aliases: BTreeMap<ScopedHistorySource, ScopedHistorySource>,
    pub(crate) replay_item_aliases: BTreeSet<(String, String, String)>,
    /// Exact checkpoint identities in this closure. Summary and operation
    /// payloads are deliberately not loaded during discovery.
    pub(crate) checkpoints: BTreeSet<SourceRef>,
}

pub(crate) struct PreparedCheckpointMetadata {
    pub(crate) emergency_inputs: BTreeSet<ScopedHistorySource>,
    pub(crate) coverage_domain: pioneer_compaction::CoverageDomain,
}

struct CachedGraph {
    graph: Arc<ResolvedCheckpointGraph>,
    units: usize,
}

struct CachedBody {
    body: Arc<CheckpointBody>,
    bytes: usize,
}

/// Metadata reuse belongs to one context preparation. Root closures are LRU
/// bounded by their aggregate retained entries, while selected summary bodies
/// are independently byte-bounded. A cached closure is historical boundary
/// metadata; every reuse still rechecks the published root and accepted scope.
pub(crate) struct CheckpointGraphResolver {
    #[cfg(test)]
    test_identity: usize,
    edges: BTreeMap<(String, SourceRef), CheckpointEdges>,
    edges_by_id: BTreeMap<(String, String), CheckpointEdges>,
    graphs: BTreeMap<(String, SourceRef), CachedGraph>,
    graph_lru: VecDeque<(String, SourceRef)>,
    graph_units: usize,
    max_graph_units: usize,
    metadata: BTreeMap<(String, SourceRef), CheckpointMetadata>,
    bodies: BTreeMap<(String, SourceRef), CachedBody>,
    body_lru: VecDeque<(String, SourceRef)>,
    body_bytes: usize,
    max_body_bytes: usize,
    operation_semantics: BTreeMap<(String, String), (pioneer_compaction::CoverageDomain, bool)>,
}

impl Default for CheckpointGraphResolver {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            test_identity: NEXT_RESOLVER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            edges: BTreeMap::new(),
            edges_by_id: BTreeMap::new(),
            graphs: BTreeMap::new(),
            graph_lru: VecDeque::new(),
            graph_units: 0,
            max_graph_units: DEFAULT_GRAPH_CACHE_UNITS,
            metadata: BTreeMap::new(),
            bodies: BTreeMap::new(),
            body_lru: VecDeque::new(),
            body_bytes: 0,
            max_body_bytes: DEFAULT_PAYLOAD_CACHE_BYTES,
            operation_semantics: BTreeMap::new(),
        }
    }
}

impl CheckpointGraphResolver {
    #[cfg(test)]
    pub(crate) fn merge(&mut self, other: Self) {
        self.edges.extend(other.edges);
        self.edges_by_id.extend(other.edges_by_id);
        self.metadata.extend(other.metadata);
        self.operation_semantics.extend(other.operation_semantics);
        for (key, cached) in other.graphs {
            self.insert_graph(key, cached.graph);
        }
        for (key, cached) in other.bodies {
            self.insert_body(key, cached.body);
        }
    }

    fn graph_units(graph: &ResolvedCheckpointGraph) -> usize {
        graph
            .leaves
            .len()
            .saturating_add(graph.replay_aliases.len())
            .saturating_add(graph.replay_item_aliases.len())
            .saturating_add(graph.checkpoints.len())
    }

    fn touch_graph(&mut self, key: &(String, SourceRef)) {
        if let Some(index) = self.graph_lru.iter().position(|candidate| candidate == key) {
            self.graph_lru.remove(index);
        }
        self.graph_lru.push_back(key.clone());
    }

    fn insert_graph(&mut self, key: (String, SourceRef), graph: Arc<ResolvedCheckpointGraph>) {
        let units = Self::graph_units(&graph);
        if units > self.max_graph_units {
            return;
        }
        if let Some(previous) = self.graphs.remove(&key) {
            self.graph_units = self.graph_units.saturating_sub(previous.units);
        }
        if let Some(index) = self
            .graph_lru
            .iter()
            .position(|candidate| candidate == &key)
        {
            self.graph_lru.remove(index);
        }
        while self.graph_units.saturating_add(units) > self.max_graph_units {
            let Some(evicted) = self.graph_lru.pop_front() else {
                break;
            };
            if let Some(previous) = self.graphs.remove(&evicted) {
                self.graph_units = self.graph_units.saturating_sub(previous.units);
            }
        }
        self.graph_units = self.graph_units.saturating_add(units);
        self.graph_lru.push_back(key.clone());
        self.graphs.insert(key, CachedGraph { graph, units });
    }

    fn body_bytes(body: &CheckpointBody) -> usize {
        body.summary.len().saturating_add(256).max(1)
    }

    fn touch_body(&mut self, key: &(String, SourceRef)) {
        if let Some(index) = self.body_lru.iter().position(|candidate| candidate == key) {
            self.body_lru.remove(index);
        }
        self.body_lru.push_back(key.clone());
    }

    fn insert_body(&mut self, key: (String, SourceRef), body: Arc<CheckpointBody>) {
        let bytes = Self::body_bytes(&body);
        if bytes > self.max_body_bytes {
            return;
        }
        if let Some(previous) = self.bodies.remove(&key) {
            self.body_bytes = self.body_bytes.saturating_sub(previous.bytes);
        }
        if let Some(index) = self.body_lru.iter().position(|candidate| candidate == &key) {
            self.body_lru.remove(index);
        }
        while self.body_bytes.saturating_add(bytes) > self.max_body_bytes {
            let Some(evicted) = self.body_lru.pop_front() else {
                break;
            };
            if let Some(previous) = self.bodies.remove(&evicted) {
                self.body_bytes = self.body_bytes.saturating_sub(previous.bytes);
            }
        }
        self.body_bytes = self.body_bytes.saturating_add(bytes);
        self.body_lru.push_back(key.clone());
        self.bodies.insert(key, CachedBody { body, bytes });
    }

    /// Metadata-only ancestry lookup. A checkpoint reached this way is not
    /// authorized for projection until an exact SourceRef is resolved below.
    pub(crate) async fn ancestry_edges(
        &mut self,
        store: &CrudStore,
        workspace: &str,
        id: &str,
    ) -> Result<CheckpointEdges> {
        self.edges_for_id(store, workspace, id).await
    }

    async fn load_edges(
        &self,
        store: &CrudStore,
        workspace: &str,
        id: &str,
        missing: &'static str,
    ) -> Result<CheckpointEdges> {
        self.observe_edge_load(store, workspace);
        let edges = store
            .compaction_checkpoint_edges(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!(missing))?;
        ensure!(
            edges.workspace_id == workspace,
            "checkpoint metadata belongs to another workspace"
        );
        Ok(edges)
    }

    async fn edges_for_source(
        &mut self,
        store: &CrudStore,
        workspace: &str,
        source: &SourceRef,
    ) -> Result<CheckpointEdges> {
        let key = (workspace.to_owned(), source.clone());
        if let Some(edges) = self.edges.get(&key) {
            return Ok(edges.clone());
        }
        let id_key = (workspace.to_owned(), source.id.clone());
        let mut edges = if let Some(edges) = self.edges_by_id.get(&id_key) {
            edges.clone()
        } else {
            self.load_edges(
                store,
                workspace,
                &source.id,
                "checkpoint coverage node disappeared",
            )
            .await?
        };
        if source.scope != format!("checkpoint:{}", edges.owner)
            || source.version != edges.identity_sha256
        {
            // An ID may have changed between preparation stages. Reloading is
            // metadata-only; the exact SourceRef check still decides validity.
            edges = self
                .load_edges(
                    store,
                    workspace,
                    &source.id,
                    "checkpoint coverage node disappeared",
                )
                .await?;
        }
        ensure!(
            source.scope == format!("checkpoint:{}", edges.owner)
                && source.version == edges.identity_sha256
                && edges.format_version == pioneer_compaction::FORMAT_VERSION,
            "checkpoint coverage owner, identity or format mismatch"
        );
        self.edges_by_id.insert(id_key, edges.clone());
        self.edges.insert(key, edges.clone());
        Ok(edges)
    }

    async fn edges_for_id(
        &mut self,
        store: &CrudStore,
        workspace: &str,
        id: &str,
    ) -> Result<CheckpointEdges> {
        let key = (workspace.to_owned(), id.to_owned());
        if let Some(edges) = self.edges_by_id.get(&key) {
            return Ok(edges.clone());
        }
        let edges = self
            .load_edges(store, workspace, id, "previous checkpoint disappeared")
            .await?;
        self.edges_by_id.insert(key, edges.clone());
        Ok(edges)
    }

    async fn discover(
        &mut self,
        store: &CrudStore,
        workspace: &str,
        allowed: Option<&BTreeSet<String>>,
        root: &SourceRef,
    ) -> Result<Option<Arc<ResolvedCheckpointGraph>>> {
        self.observe_closure_build(store, workspace);
        let mut leaves = BTreeSet::new();
        let mut replay_aliases = BTreeMap::new();
        let mut replay_item_aliases = BTreeSet::new();
        let mut checkpoints = BTreeSet::new();
        let mut done = BTreeSet::new();
        let mut visiting = BTreeSet::new();
        let Some(root_thread) = store.compaction_reference_thread(workspace, root).await? else {
            return Ok(None);
        };
        let mut pending = vec![(root.clone(), root_thread, false)];
        let mut visited = 0usize;
        while let Some((source, thread, exiting)) = pending.pop() {
            if exiting {
                visiting.remove(&source);
                done.insert(source);
                continue;
            }
            if done.contains(&source) {
                continue;
            }
            visited = visited
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("checkpoint graph size overflow"))?;
            ensure!(
                visited <= MAX_HISTORICAL_GRAPH_NODES,
                "checkpoint historical coverage exceeds supported quantum"
            );
            if source == *root {
                ensure!(
                    allowed.is_none_or(|allowed| allowed.contains(&thread)),
                    "checkpoint root is outside the accepted source scope"
                );
            }
            if !source.scope.starts_with("checkpoint:") {
                leaves.insert(ScopedHistorySource {
                    thread,
                    source: source.clone(),
                });
                done.insert(source);
                continue;
            }
            ensure!(
                visiting.insert(source.clone()),
                "cyclic checkpoint coverage"
            );
            let edges = self.edges_for_source(store, workspace, &source).await?;
            ensure!(
                edges.workspace_id == workspace && edges.thread_id == thread,
                "checkpoint historical ownership changed"
            );
            checkpoints.insert(source.clone());
            for alias in edges.replay_aliases.iter().cloned() {
                if let (Some(turn), Some(item)) = (
                    alias.covered.source.scope.strip_prefix("item:"),
                    alias.tool_item_id.clone(),
                ) {
                    replay_item_aliases.insert((
                        alias.covered.source_thread.clone(),
                        turn.to_owned(),
                        item,
                    ));
                }
                let replay = ScopedHistorySource {
                    thread: alias.replay.source_thread,
                    source: alias.replay.source,
                };
                let covered = ScopedHistorySource {
                    thread: alias.covered.source_thread,
                    source: alias.covered.source,
                };
                if let Some(previous) = replay_aliases.insert(replay, covered.clone()) {
                    ensure!(previous == covered, "checkpoint replay alias is ambiguous");
                }
            }
            pending.push((source, thread.clone(), true));
            if let Some(previous_id) = &edges.previous {
                let previous_edges = self.edges_for_id(store, workspace, previous_id).await?;
                ensure!(
                    previous_edges.owner == edges.owner
                        && previous_edges.workspace_id == edges.workspace_id
                        && previous_edges.thread_id == edges.thread_id
                        && previous_edges.format_version == pioneer_compaction::FORMAT_VERSION,
                    "previous checkpoint changed historical ownership or format"
                );
                let previous = SourceRef {
                    scope: format!("checkpoint:{}", previous_edges.owner),
                    id: previous_id.clone(),
                    version: previous_edges.identity_sha256,
                };
                pending.push((previous, previous_edges.thread_id, false));
            }
            pending.extend(
                edges
                    .coverage
                    .into_iter()
                    .map(|covered| (covered.source, covered.source_thread, false)),
            );
        }
        ensure!(!leaves.is_empty(), "checkpoint has no historical coverage");
        Ok(Some(Arc::new(ResolvedCheckpointGraph {
            leaves,
            replay_aliases,
            replay_item_aliases,
            checkpoints,
        })))
    }

    pub(crate) async fn resolve(
        &mut self,
        store: &CrudStore,
        workspace: &str,
        allowed: Option<&BTreeSet<String>>,
        root: &SourceRef,
    ) -> Result<Option<Arc<ResolvedCheckpointGraph>>> {
        let key = (workspace.to_owned(), root.clone());
        if let Some(graph) = self.graphs.get(&key).map(|cached| cached.graph.clone()) {
            let Some(thread) = store.compaction_reference_thread(workspace, root).await? else {
                return Ok(None);
            };
            ensure!(
                allowed.is_none_or(|allowed| allowed.contains(&thread)),
                "checkpoint root is outside the accepted source scope"
            );
            self.touch_graph(&key);
            return Ok(Some(graph));
        }
        let graph = self.discover(store, workspace, allowed, root).await?;
        if let Some(graph) = &graph {
            self.insert_graph(key, graph.clone());
        }
        Ok(graph)
    }

    /// Verify a checkpoint input against an exact immutable grant frontier.
    /// A granted checkpoint is atomic: traversal stops at that node, so the
    /// grant neither depends on nor exposes its historical raw descendants.
    pub(crate) async fn authorized_by_historical_inputs(
        &mut self,
        store: &CrudStore,
        workspace: &str,
        root: &SourceRef,
        grants: &BTreeSet<ScopedHistorySource>,
    ) -> Result<bool> {
        let Some(root_thread) = store.compaction_reference_thread(workspace, root).await? else {
            return Ok(false);
        };
        let mut pending = vec![(root.clone(), root_thread, false)];
        let mut done = BTreeSet::new();
        let mut visiting = BTreeSet::new();
        let mut used = BTreeSet::new();
        let mut visited = 0usize;
        while let Some((source, thread, exiting)) = pending.pop() {
            let key = ScopedHistorySource {
                thread: thread.clone(),
                source: source.clone(),
            };
            if exiting {
                visiting.remove(&key);
                done.insert(key);
                continue;
            }
            if done.contains(&key) {
                continue;
            }
            visited = visited
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("checkpoint graph size overflow"))?;
            ensure!(
                visited <= MAX_HISTORICAL_GRAPH_NODES,
                "checkpoint historical coverage exceeds supported quantum"
            );
            if grants.contains(&key) {
                if source.scope.starts_with("checkpoint:")
                    && store
                        .compaction_reference_thread(workspace, &source)
                        .await?
                        .as_deref()
                        != Some(&thread)
                {
                    return Ok(false);
                }
                used.insert(key.clone());
                done.insert(key);
                continue;
            }
            if !source.scope.starts_with("checkpoint:") {
                return Ok(false);
            }
            ensure!(visiting.insert(key.clone()), "cyclic checkpoint coverage");
            let edges = self.edges_for_source(store, workspace, &source).await?;
            ensure!(
                edges.workspace_id == workspace && edges.thread_id == thread,
                "checkpoint historical ownership changed"
            );
            pending.push((source, thread.clone(), true));
            if let Some(previous_id) = &edges.previous {
                let previous_edges = self.edges_for_id(store, workspace, previous_id).await?;
                ensure!(
                    previous_edges.owner == edges.owner
                        && previous_edges.workspace_id == edges.workspace_id
                        && previous_edges.thread_id == edges.thread_id
                        && previous_edges.format_version == pioneer_compaction::FORMAT_VERSION,
                    "previous checkpoint changed historical ownership or format"
                );
                pending.push((
                    SourceRef {
                        scope: format!("checkpoint:{}", previous_edges.owner),
                        id: previous_id.clone(),
                        version: previous_edges.identity_sha256,
                    },
                    previous_edges.thread_id,
                    false,
                ));
            }
            pending.extend(
                edges
                    .coverage
                    .into_iter()
                    .map(|covered| (covered.source, covered.source_thread, false)),
            );
        }
        Ok(used == *grants)
    }

    async fn metadata_for_source(
        &mut self,
        store: &CrudStore,
        workspace: &str,
        source: &SourceRef,
    ) -> Result<CheckpointMetadata> {
        let key = (workspace.to_owned(), source.clone());
        if let Some(metadata) = self.metadata.get(&key) {
            return Ok(metadata.clone());
        }
        let metadata = store
            .compaction_checkpoint_metadata(&source.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("checkpoint coverage link is missing"))?;
        ensure!(
            source.scope == format!("checkpoint:{}", metadata.owner)
                && source.version == metadata.identity_sha256
                && metadata.format_version == pioneer_compaction::FORMAT_VERSION,
            "checkpoint owner, identity or format mismatch"
        );
        let edges = self.edges_for_source(store, workspace, source).await?;
        ensure!(
            metadata.owner == edges.owner
                && metadata.previous == edges.previous
                && metadata.identity_sha256 == edges.identity_sha256
                && metadata.format_version == edges.format_version,
            "checkpoint metadata changed from prepared graph edges"
        );
        self.metadata.insert(key, metadata.clone());
        Ok(metadata)
    }

    async fn body_for_source(
        &mut self,
        store: &CrudStore,
        workspace: &str,
        source: &SourceRef,
        metadata: &CheckpointMetadata,
    ) -> Result<Arc<CheckpointBody>> {
        let key = (workspace.to_owned(), source.clone());
        if let Some(body) = self.bodies.get(&key).map(|cached| cached.body.clone()) {
            self.touch_body(&key);
            return Ok(body);
        }
        self.observe_body_load(store, workspace);
        self.pause_body_load(store, workspace, &source.id).await;
        let body = Arc::new(
            store
                .compaction_checkpoint_body(&source.id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("checkpoint source is missing"))?,
        );
        ensure!(
            body.id == metadata.id
                && body.operation_id == metadata.operation_id
                && body.owner == metadata.owner
                && body.previous == metadata.previous
                && body.identity_sha256 == metadata.identity_sha256
                && body.selection == metadata.selection
                && body.projection_version == metadata.projection_version
                && body.format_version == metadata.format_version,
            "checkpoint body changed from prepared metadata"
        );
        self.insert_body(key, body.clone());
        Ok(body)
    }

    #[cfg(test)]
    async fn pause_body_load(&self, store: &CrudStore, workspace: &str, checkpoint: &str) {
        let state = BODY_LOAD_PAUSES
            .lock()
            .unwrap()
            .get(&(
                store.database_connection().runtime_identity(),
                workspace.to_owned(),
                checkpoint.to_owned(),
            ))
            .and_then(std::sync::Weak::upgrade);
        let Some(state) = state else {
            return;
        };
        state
            .reached
            .store(true, std::sync::atomic::Ordering::SeqCst);
        state.reached_notify.notify_waiters();
        loop {
            let notified = state.release_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if state.released.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            notified.await;
        }
    }

    #[cfg(not(test))]
    async fn pause_body_load(&self, _store: &CrudStore, _workspace: &str, _checkpoint: &str) {}

    pub(crate) async fn projection_metadata(
        &mut self,
        store: &CrudStore,
        workspace: &str,
        graph: &ResolvedCheckpointGraph,
    ) -> Result<PreparedCheckpointMetadata> {
        let mut emergency_inputs = BTreeSet::new();
        let mut coverage_domain = pioneer_compaction::CoverageDomain::OwnContribution;
        for source in &graph.checkpoints {
            let metadata = self.metadata_for_source(store, workspace, source).await?;
            let operation_key = (workspace.to_owned(), metadata.operation_id.clone());
            let (node_domain, emergency) =
                if let Some(semantics) = self.operation_semantics.get(&operation_key) {
                    *semantics
                } else {
                    let operation = store
                        .compaction_operation(&metadata.operation_id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("checkpoint operation is missing"))?;
                    let snapshot: OperationSnapshot = serde_json::from_str(&operation.snapshot)?;
                    let semantics = (
                        snapshot.plan.coverage_domain,
                        snapshot.plan.mode == pioneer_compaction::CompactionMode::Emergency,
                    );
                    self.operation_semantics.insert(operation_key, semantics);
                    semantics
                };
            if node_domain == pioneer_compaction::CoverageDomain::WorkingContext {
                coverage_domain = pioneer_compaction::CoverageDomain::WorkingContext;
            }
            if emergency {
                let edges = self.edges_for_source(store, workspace, source).await?;
                emergency_inputs.extend(
                    edges
                        .coverage
                        .into_iter()
                        .filter(|covered| covered.source.scope.starts_with("input:"))
                        .map(|covered| ScopedHistorySource {
                            thread: covered.source_thread,
                            source: covered.source,
                        }),
                );
            }
        }
        Ok(PreparedCheckpointMetadata {
            emergency_inputs,
            coverage_domain,
        })
    }

    pub(crate) async fn projection_body(
        &mut self,
        store: &CrudStore,
        workspace: &str,
        source: &SourceRef,
    ) -> Result<Arc<CheckpointBody>> {
        let metadata = self.metadata_for_source(store, workspace, source).await?;
        self.body_for_source(store, workspace, source, &metadata)
            .await
    }

    #[cfg(test)]
    pub(crate) fn with_cache_limits(max_graph_units: usize, max_body_bytes: usize) -> Self {
        Self {
            max_graph_units,
            max_body_bytes,
            ..Self::default()
        }
    }

    #[cfg(test)]
    pub(crate) fn cached_graph_units(&self) -> usize {
        self.graph_units
    }

    #[cfg(test)]
    pub(crate) fn test_identity(&self) -> usize {
        self.test_identity
    }

    #[cfg(test)]
    pub(crate) fn cached_graph_roots(&self) -> usize {
        self.graphs.len()
    }

    #[cfg(test)]
    pub(crate) fn cached_payload_bytes(&self) -> usize {
        self.body_bytes
    }

    #[cfg(test)]
    fn observe(
        &self,
        store: &CrudStore,
        workspace: &str,
        field: impl FnOnce(&PreparationWork) -> &std::sync::atomic::AtomicUsize,
    ) {
        if let Some(work) = PREPARATION_WORK
            .lock()
            .unwrap()
            .get(&(
                store.database_connection().runtime_identity(),
                workspace.to_owned(),
            ))
            .and_then(std::sync::Weak::upgrade)
        {
            field(&work).fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[cfg(not(test))]
    fn observe_edge_load(&self, _store: &CrudStore, _workspace: &str) {}

    #[cfg(not(test))]
    fn observe_body_load(&self, _store: &CrudStore, _workspace: &str) {}

    #[cfg(not(test))]
    fn observe_closure_build(&self, _store: &CrudStore, _workspace: &str) {}

    #[cfg(test)]
    fn observe_edge_load(&self, store: &CrudStore, workspace: &str) {
        self.observe(store, workspace, |work| &work.edge_loads);
    }

    #[cfg(test)]
    fn observe_body_load(&self, store: &CrudStore, workspace: &str) {
        self.observe(store, workspace, |work| &work.body_loads);
    }

    #[cfg(test)]
    fn observe_closure_build(&self, store: &CrudStore, workspace: &str) {
        self.observe(store, workspace, |work| &work.closure_builds);
    }
}

#[cfg(test)]
pub(crate) async fn checkpoint_leaves(
    store: &CrudStore,
    workspace: &str,
    allowed: &BTreeSet<String>,
    root: &SourceRef,
) -> Result<BTreeSet<ScopedHistorySource>> {
    CheckpointGraphResolver::default()
        .resolve(store, workspace, Some(allowed), root)
        .await?
        .ok_or_else(|| anyhow::anyhow!("checkpoint root is unavailable"))
        .map(|graph| graph.leaves.clone())
}

#[cfg(test)]
mod cache_tests {
    use super::*;

    fn source(id: &str) -> SourceRef {
        SourceRef {
            scope: "checkpoint:owner".into(),
            id: id.into(),
            version: format!("identity-{id}"),
        }
    }

    fn graph(id: &str, item_aliases: usize) -> Arc<ResolvedCheckpointGraph> {
        Arc::new(ResolvedCheckpointGraph {
            leaves: BTreeSet::from([ScopedHistorySource {
                thread: "thread".into(),
                source: SourceRef {
                    scope: "event:turn".into(),
                    id: format!("leaf-{id}"),
                    version: "event-revision:1".into(),
                },
            }]),
            replay_aliases: BTreeMap::new(),
            replay_item_aliases: (0..item_aliases)
                .map(|index| ("thread".into(), "turn".into(), format!("item-{id}-{index}")))
                .collect(),
            checkpoints: BTreeSet::from([source(id)]),
        })
    }

    #[test]
    fn item_alias_units_reject_oversize_closures_and_drive_lru_eviction() {
        let mut resolver = CheckpointGraphResolver::with_cache_limits(4, 1);
        let first_key = ("ws".into(), source("first"));
        let first = graph("first", 0);
        resolver.insert_graph(first_key.clone(), first.clone());
        assert_eq!(resolver.graph_units, 2);
        assert!(resolver.graphs.contains_key(&first_key));

        let second_key = ("ws".into(), source("second"));
        let second = graph("second", 2);
        resolver.insert_graph(second_key.clone(), second);
        assert_eq!(resolver.graph_units, 4);
        assert!(!resolver.graphs.contains_key(&first_key));
        assert!(resolver.graphs.contains_key(&second_key));
        assert_eq!(
            first.leaves.len(),
            1,
            "eviction must not invalidate caller Arc"
        );

        let oversize_key = ("ws".into(), source("oversize"));
        let oversize = graph("oversize", 3);
        assert_eq!(CheckpointGraphResolver::graph_units(&oversize), 5);
        resolver.insert_graph(oversize_key.clone(), oversize.clone());
        assert!(!resolver.graphs.contains_key(&oversize_key));
        assert!(resolver.graphs.contains_key(&second_key));
        assert_eq!(oversize.replay_item_aliases.len(), 3);
    }
}
