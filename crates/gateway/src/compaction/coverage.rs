//! Validate the entire retained checkpoint DAG, including accepted foreign work.
//! A local context epoch alone cannot detect edits in another source thread.
use super::*;
use pioneer_agent::compaction::composition::ScopedHistorySource;
use pioneer_crud::compaction::{CheckpointBody, CheckpointEdges, CheckpointMetadata};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

const DEFAULT_GRAPH_CACHE_UNITS: usize = 4 * 1024;
const DEFAULT_PAYLOAD_CACHE_BYTES: usize = 256 * 1024;

#[cfg(test)]
static NEXT_RESOLVER_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

#[cfg(test)]
#[derive(Default)]
struct PreparationWork {
    edge_loads: std::sync::atomic::AtomicUsize,
    body_loads: std::sync::atomic::AtomicUsize,
    closure_builds: std::sync::atomic::AtomicUsize,
    revalidations: std::sync::atomic::AtomicUsize,
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

    pub(crate) fn revalidations(&self) -> usize {
        self.work
            .revalidations
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparationWorkSnapshot {
    pub(crate) edge_loads: usize,
    pub(crate) body_loads: usize,
    pub(crate) closure_builds: usize,
    pub(crate) revalidations: usize,
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
            revalidations: work.revalidations.load(std::sync::atomic::Ordering::SeqCst),
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

#[derive(Clone)]
struct ResolvedGraphSource {
    source: SourceRef,
    thread: String,
    strict_current: bool,
}

pub(crate) struct ResolvedCheckpointGraph {
    pub(crate) leaves: BTreeSet<ScopedHistorySource>,
    pub(crate) scopes: BTreeSet<String>,
    /// Exact checkpoint identities in this closure. Summary and operation
    /// payloads are deliberately not loaded during discovery.
    pub(crate) checkpoints: BTreeSet<SourceRef>,
    sources: Vec<ResolvedGraphSource>,
}

pub(crate) struct PreparedCheckpointMetadata {
    pub(crate) emergency_inputs: BTreeSet<SourceRef>,
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
/// are independently byte-bounded. Eviction only loses optimization state.
/// Every reuse separately revalidates exact source identities and allowed
/// threads, so cached metadata is never an authorization or publication grant.
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
            .sources
            .len()
            .saturating_add(graph.leaves.len())
            .saturating_add(graph.scopes.len())
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
        store
            .compaction_checkpoint_edges(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!(missing))
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
        let mut scopes = BTreeSet::new();
        let mut checkpoints = BTreeSet::new();
        let mut threads = BTreeMap::<SourceRef, String>::new();
        let mut strict = BTreeSet::new();
        let mut done = BTreeSet::new();
        let mut visiting = BTreeSet::new();
        let mut pending = vec![(root.clone(), false, None)];
        while let Some((source, exiting, known_thread)) = pending.pop() {
            if exiting {
                visiting.remove(&source);
                done.insert(source);
                continue;
            }
            if done.contains(&source) {
                continue;
            }
            let thread = if let Some(thread) = known_thread {
                thread
            } else {
                let Some(thread) = store
                    .compaction_reference_thread(workspace, &source)
                    .await?
                else {
                    return Ok(None);
                };
                thread
            };
            ensure!(
                allowed.is_none_or(|allowed| allowed.contains(&thread)),
                "checkpoint coverage crosses the accepted source scope"
            );
            if let Some(previous) = threads.insert(source.clone(), thread.clone()) {
                ensure!(previous == thread, "checkpoint source changed thread");
            }
            scopes.insert(thread.clone());
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
            checkpoints.insert(source.clone());
            pending.push((source, true, None));
            if let Some(previous_id) = &edges.previous {
                let previous_edges = self.edges_for_id(store, workspace, previous_id).await?;
                ensure!(
                    previous_edges.owner == edges.owner
                        && previous_edges.format_version == pioneer_compaction::FORMAT_VERSION,
                    "previous checkpoint changed owner or format"
                );
                let previous = SourceRef {
                    scope: format!("checkpoint:{}", previous_edges.owner),
                    id: previous_id.clone(),
                    version: previous_edges.identity_sha256,
                };
                let previous_thread = store
                    .compaction_reference_thread(workspace, &previous)
                    .await?;
                ensure!(
                    previous_thread.as_deref() == Some(thread.as_str()),
                    "previous checkpoint changed scope or publication status"
                );
                strict.insert(previous.clone());
                pending.push((previous, false, previous_thread));
            }
            pending.extend(
                edges
                    .coverage
                    .into_iter()
                    .map(|source| (source, false, None)),
            );
        }
        ensure!(
            !leaves.is_empty(),
            "checkpoint has no exact canonical coverage"
        );
        let sources = threads
            .into_iter()
            .map(|(source, thread)| ResolvedGraphSource {
                strict_current: strict.contains(&source),
                source,
                thread,
            })
            .collect();
        Ok(Some(Arc::new(ResolvedCheckpointGraph {
            leaves,
            scopes,
            checkpoints,
            sources,
        })))
    }

    async fn revalidate(
        &self,
        store: &CrudStore,
        workspace: &str,
        allowed: Option<&BTreeSet<String>>,
        graph: &ResolvedCheckpointGraph,
    ) -> Result<bool> {
        self.observe_revalidation(store, workspace);
        for resolved in &graph.sources {
            let current = store
                .compaction_reference_thread(workspace, &resolved.source)
                .await?;
            if resolved.strict_current {
                ensure!(
                    current.as_deref() == Some(resolved.thread.as_str()),
                    "previous checkpoint changed scope or publication status"
                );
            } else if current.as_deref() != Some(resolved.thread.as_str()) {
                return Ok(false);
            }
            ensure!(
                allowed.is_none_or(|allowed| allowed.contains(&resolved.thread)),
                "checkpoint coverage crosses the accepted source scope"
            );
        }
        Ok(true)
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
            self.touch_graph(&key);
            return Ok(self
                .revalidate(store, workspace, allowed, &graph)
                .await?
                .then_some(graph));
        }
        let graph = self.discover(store, workspace, allowed, root).await?;
        if let Some(graph) = &graph {
            self.insert_graph(key, graph.clone());
        }
        Ok(graph)
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
                        .filter(|covered| covered.scope.starts_with("input:")),
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

    #[cfg(not(test))]
    fn observe_revalidation(&self, _store: &CrudStore, _workspace: &str) {}

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

    #[cfg(test)]
    fn observe_revalidation(&self, store: &CrudStore, workspace: &str) {
        self.observe(store, workspace, |work| &work.revalidations);
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
        .ok_or_else(|| anyhow::anyhow!("checkpoint coverage source changed or disappeared"))
        .map(|graph| graph.leaves.clone())
}
