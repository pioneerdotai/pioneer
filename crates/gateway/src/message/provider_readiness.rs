//! Gateway-owned provider readiness lifecycle.
//!
//! Client list/status requests never start provider probes. They read
//! [`ProviderReadinessSupervisor`] snapshots; before the supervisor's first
//! publication, concurrent readers may coalesce around one cheap
//! `initializing` catalog seed. The single worker below exclusively owns API
//! warm-up and CLI account/MCP probes, coalescing internal lifecycle triggers
//! per workspace and publishing changes back to clients as status
//! notifications.

use super::*;
use futures_util::{FutureExt, future::join_all};
use pioneer_observability::{
    GatewayCliRuntimeKind, GatewayProviderReadinessState, GatewayProviderWarmupScope,
    GatewayProviderWarmupStage, GatewayProviderWarmupTrace,
};
use pioneer_protocol::{CLIAgentRuntimeKind, RuntimeStatus};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore, mpsc};
use tokio::task::{JoinHandle, JoinSet};

/// Quickly revisits only enabled CLI runtimes that have not reached `ready`.
const PROVIDER_READINESS_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const PROVIDER_READINESS_RETRY_MAX_DELAY: Duration = Duration::from_secs(5 * 60);
/// Detects readiness regressions even when no settings or account event fires.
const PROVIDER_READINESS_RECONCILE_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// No provider is allowed to occupy a global probe permit indefinitely.
const PROVIDER_READINESS_PROBE_TIMEOUT: Duration = Duration::from_secs(30);
/// CLI readiness includes account, MCP, and model-catalog checks in one
/// Gateway-owned operation and therefore receives a larger aggregate budget.
const CLI_RUNTIME_READINESS_PROBE_TIMEOUT: Duration = Duration::from_secs(60);
/// Keeps a Gateway with many workspaces from spawning an unbounded number of
/// account, MCP, DNS, and TLS probes at once.
const MAX_CONCURRENT_WORKSPACE_WARMUPS: usize = 4;
/// Bounds actual provider network/process probes across all warm workspaces.
const MAX_CONCURRENT_PROVIDER_PROBES: usize = 8;

type ProviderWarmupTaskOutcome =
    Result<anyhow::Result<Option<ProviderWarmupScope>>, Box<dyn std::any::Any + Send + 'static>>;
type ProviderWarmupTaskCompletion = (String, ProviderWarmupScope, ProviderWarmupTaskOutcome);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CliRuntimeWarmupOutcome {
    Ready,
    Retry,
    Superseded,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProviderWarmupExecution {
    retry_scope: Option<ProviderWarmupScope>,
    superseded: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProviderWarmupScope {
    ApiOnly,
    CliOnly,
    All,
}

impl ProviderWarmupScope {
    const fn includes_api(self) -> bool {
        matches!(self, Self::ApiOnly | Self::All)
    }

    const fn includes_cli(self) -> bool {
        matches!(self, Self::CliOnly | Self::All)
    }

    const fn merge(self, other: Self) -> Self {
        if matches!(
            (self, other),
            (Self::ApiOnly, Self::ApiOnly)
                | (Self::CliOnly, Self::CliOnly)
                | (Self::All, Self::All)
        ) {
            self
        } else {
            Self::All
        }
    }

    const fn telemetry_scope(self) -> GatewayProviderWarmupScope {
        match self {
            Self::ApiOnly => GatewayProviderWarmupScope::Api,
            Self::CliOnly => GatewayProviderWarmupScope::Cli,
            Self::All => GatewayProviderWarmupScope::All,
        }
    }

    const fn subtract(self, completed: Self) -> Option<Self> {
        match (self, completed) {
            (_, Self::All) | (Self::ApiOnly, Self::ApiOnly) | (Self::CliOnly, Self::CliOnly) => {
                None
            }
            (Self::All, Self::ApiOnly) => Some(Self::CliOnly),
            (Self::All, Self::CliOnly) => Some(Self::ApiOnly),
            (remaining, _) => Some(remaining),
        }
    }

    const fn from_retry_needs(api: bool, cli: bool) -> Option<Self> {
        match (api, cli) {
            (true, true) => Some(Self::All),
            (true, false) => Some(Self::ApiOnly),
            (false, true) => Some(Self::CliOnly),
            (false, false) => None,
        }
    }
}

#[derive(Clone, Debug)]
enum ProviderWarmupCommand {
    AllWorkspaces {
        scope: ProviderWarmupScope,
        force_if_running: bool,
    },
    Workspace {
        workspace_id: String,
        scope: ProviderWarmupScope,
        force_if_running: bool,
    },
}

#[derive(Clone, Copy, Debug)]
struct PendingProviderWarmup {
    scope: ProviderWarmupScope,
    force_if_running: bool,
}

impl PendingProviderWarmup {
    fn merge(&mut self, scope: ProviderWarmupScope, force_if_running: bool) {
        self.scope = self.scope.merge(scope);
        self.force_if_running |= force_if_running;
    }
}

/// Bounded command accumulator for the supervisor.
///
/// A client burst can contribute at most one pending entry per workspace and
/// one global entry. The wake-up channel has capacity one, so repeated
/// lifecycle triggers never build an unbounded queue while the worker is busy.
#[derive(Debug, Default)]
struct ProviderWarmupInbox {
    all_workspaces: Option<PendingProviderWarmup>,
    workspaces: HashMap<String, PendingProviderWarmup>,
}

impl ProviderWarmupInbox {
    fn push(&mut self, command: ProviderWarmupCommand) {
        match command {
            ProviderWarmupCommand::AllWorkspaces {
                scope,
                force_if_running,
            } => match self.all_workspaces.as_mut() {
                Some(pending) => pending.merge(scope, force_if_running),
                None => {
                    self.all_workspaces = Some(PendingProviderWarmup {
                        scope,
                        force_if_running,
                    });
                }
            },
            ProviderWarmupCommand::Workspace {
                workspace_id,
                scope,
                force_if_running,
            } => match self.workspaces.get_mut(workspace_id.as_str()) {
                Some(pending) => pending.merge(scope, force_if_running),
                None => {
                    self.workspaces.insert(
                        workspace_id,
                        PendingProviderWarmup {
                            scope,
                            force_if_running,
                        },
                    );
                }
            },
        }
    }

    fn drain(&mut self) -> Vec<ProviderWarmupCommand> {
        let mut commands = Vec::with_capacity(self.workspaces.len() + 1);
        if let Some(pending) = self.all_workspaces.take() {
            commands.push(ProviderWarmupCommand::AllWorkspaces {
                scope: pending.scope,
                force_if_running: pending.force_if_running,
            });
        }
        commands.extend(self.workspaces.drain().map(|(workspace_id, pending)| {
            ProviderWarmupCommand::Workspace {
                workspace_id,
                scope: pending.scope,
                force_if_running: pending.force_if_running,
            }
        }));
        commands
    }

    fn clear(&mut self) {
        self.all_workspaces = None;
        self.workspaces.clear();
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct CliRuntimeReadinessSnapshot {
    pub(super) revision: u64,
    pub(super) runtimes: Vec<RuntimeSummary>,
    models: HashMap<String, CliRuntimeModelSnapshot>,
    mcp_readiness: HashMap<String, pioneer_protocol::CliMcpAdapterReadiness>,
}

#[derive(Clone, Debug)]
struct CliRuntimeReadinessChange {
    revision: u64,
    runtime: RuntimeSummary,
    removed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct CliRuntimeModelSnapshot {
    pub(super) result: super::cli_runtime::RuntimeModelListResult,
    pub(super) refreshed_at_unix_ms: i64,
}

#[derive(Clone, Debug)]
pub(super) struct CliRuntimeProbeSnapshot {
    pub(super) summary: RuntimeSummary,
    pub(super) models: Option<CliRuntimeModelSnapshot>,
    pub(super) mcp_readiness: Option<pioneer_protocol::CliMcpAdapterReadiness>,
}

#[derive(Clone, Debug)]
struct CliRuntimeProbeResult {
    summary: RuntimeSummary,
    models: Option<CliRuntimeModelSnapshot>,
    mcp_readiness: Option<pioneer_protocol::CliMcpAdapterReadiness>,
}

#[derive(Default)]
pub(super) struct ProviderReadinessSupervisor {
    cli_snapshots: RwLock<HashMap<String, CliRuntimeReadinessSnapshot>>,
    cli_generations: RwLock<HashMap<String, u64>>,
    cli_seed_locks: RwLock<HashMap<String, Arc<Mutex<()>>>>,
    pending_commands: StdMutex<ProviderWarmupInbox>,
    command_tx: StdMutex<Option<mpsc::Sender<()>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl ProviderReadinessSupervisor {
    fn send(&self, command: ProviderWarmupCommand) {
        let sender_guard = self
            .command_tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(sender) = sender_guard.as_ref() {
            self.pending_commands
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(command);
            // `Full` means a wake-up is already pending. `Closed` is expected
            // during shutdown and the pending state is cleared there.
            let _ = sender.try_send(());
        }
    }

    async fn snapshot(&self, workspace_id: &str) -> Option<CliRuntimeReadinessSnapshot> {
        // Readers participate in the same generation -> snapshot lock order
        // as probes and invalidation. Once invalidation acquires the
        // generation writer, no new reader can observe the previous `ready`
        // snapshot before its fail-closed replacement is visible.
        let _generation = self.cli_generations.read().await;
        self.cli_snapshots.read().await.get(workspace_id).cloned()
    }

    #[cfg(test)]
    async fn runtime_probe_snapshot(
        &self,
        workspace_id: &str,
        runtime_id: &str,
    ) -> Option<CliRuntimeProbeSnapshot> {
        // The generation read barrier prevents a configuration invalidation
        // from exposing the previous workspace value to a new reader.
        let _generation = self.cli_generations.read().await;
        let snapshots = self.cli_snapshots.read().await;
        let snapshot = snapshots.get(workspace_id)?;
        let summary = snapshot
            .runtimes
            .iter()
            .find(|runtime| runtime.runtime_id == runtime_id)?
            .clone();
        Some(CliRuntimeProbeSnapshot {
            summary,
            models: snapshot.models.get(runtime_id).cloned(),
            mcp_readiness: snapshot.mcp_readiness.get(runtime_id).cloned(),
        })
    }

    async fn cli_generation(&self, workspace_id: &str) -> u64 {
        // Register the workspace even before its first snapshot is published.
        // A global configuration invalidation can then fence an in-flight
        // first probe just as reliably as a later reconciliation.
        *self
            .cli_generations
            .write()
            .await
            .entry(workspace_id.to_owned())
            .or_default()
    }

    async fn cli_seed_lock(&self, workspace_id: &str) -> Arc<Mutex<()>> {
        self.cli_seed_locks
            .write()
            .await
            .entry(workspace_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn insert_snapshot_if_absent_if_generation(
        &self,
        workspace_id: &str,
        generation: u64,
        runtimes: Vec<RuntimeSummary>,
    ) -> Option<CliRuntimeReadinessSnapshot> {
        // Keep the generation guard through publication. A first client read
        // can race a settings mutation before the supervisor has published
        // anything; a stale catalog seed must not win that race.
        let generations = self.cli_generations.read().await;
        if generations.get(workspace_id).copied() != Some(generation) {
            return None;
        }
        let mut snapshots = self.cli_snapshots.write().await;
        Some(
            snapshots
                .entry(workspace_id.to_owned())
                .or_insert_with(|| CliRuntimeReadinessSnapshot {
                    revision: 1,
                    runtimes,
                    models: HashMap::new(),
                    mcp_readiness: HashMap::new(),
                })
                .clone(),
        )
    }

    async fn invalidate_snapshot(
        &self,
        workspace_id: &str,
    ) -> (u64, Vec<CliRuntimeReadinessChange>) {
        // Generation and snapshot always use this lock order. Holding the
        // generation write guard until the replacement snapshot is visible
        // makes invalidation atomic for every concurrent reader/probe.
        let mut generations = self.cli_generations.write().await;
        let generation = generations
            .get(workspace_id)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        generations.insert(workspace_id.to_owned(), generation);

        let runtimes = self
            .cli_snapshots
            .read()
            .await
            .get(workspace_id)
            .map(|snapshot| {
                snapshot
                    .runtimes
                    .iter()
                    .cloned()
                    .map(super::cli_runtime::cli_runtime_initializing_summary)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let changes = self.replace_snapshot(workspace_id, runtimes).await.1;
        (generation, changes)
    }

    async fn replace_snapshot(
        &self,
        workspace_id: &str,
        runtimes: Vec<RuntimeSummary>,
    ) -> (CliRuntimeReadinessSnapshot, Vec<CliRuntimeReadinessChange>) {
        self.replace_snapshot_with_probe_artifacts(
            workspace_id,
            runtimes,
            HashMap::new(),
            HashMap::new(),
        )
        .await
    }

    async fn replace_snapshot_with_probe_artifacts(
        &self,
        workspace_id: &str,
        runtimes: Vec<RuntimeSummary>,
        models: HashMap<String, CliRuntimeModelSnapshot>,
        mcp_readiness: HashMap<String, pioneer_protocol::CliMcpAdapterReadiness>,
    ) -> (CliRuntimeReadinessSnapshot, Vec<CliRuntimeReadinessChange>) {
        let mut snapshots = self.cli_snapshots.write().await;
        let had_previous = snapshots.contains_key(workspace_id);
        let previous = snapshots.get(workspace_id).cloned().unwrap_or_default();
        if previous.runtimes == runtimes {
            if !had_previous && runtimes.is_empty() && models.is_empty() && mcp_readiness.is_empty()
            {
                return (previous, Vec::new());
            }
            let snapshot = CliRuntimeReadinessSnapshot {
                revision: previous.revision,
                runtimes,
                models,
                mcp_readiness,
            };
            snapshots.insert(workspace_id.to_owned(), snapshot.clone());
            return (snapshot, Vec::new());
        }

        let previous_revision = previous.revision;
        let previous_by_id = previous
            .runtimes
            .into_iter()
            .map(|runtime| (runtime.runtime_id.clone(), runtime))
            .collect::<HashMap<_, _>>();
        let current_ids = runtimes
            .iter()
            .map(|runtime| runtime.runtime_id.clone())
            .collect::<HashSet<_>>();
        let mut changed = runtimes
            .iter()
            .filter(|runtime| previous_by_id.get(runtime.runtime_id.as_str()) != Some(*runtime))
            .cloned()
            .map(|runtime| (runtime, false))
            .collect::<Vec<_>>();
        for mut removed in previous_by_id
            .into_values()
            .filter(|runtime| !current_ids.contains(runtime.runtime_id.as_str()))
        {
            removed.enabled = false;
            removed.status = RuntimeStatus::Disabled;
            changed.push((removed, true));
        }
        let changed = changed
            .into_iter()
            .enumerate()
            .map(|(index, (runtime, removed))| CliRuntimeReadinessChange {
                // Every delta owns a distinct revision. If a bounded client
                // notification queue drops one update, the next delta exposes
                // the gap and forces a full authoritative snapshot reload.
                revision: previous_revision
                    .saturating_add(index as u64)
                    .saturating_add(1)
                    .max(1),
                runtime,
                removed,
            })
            .collect::<Vec<_>>();
        let revision = changed
            .last()
            .map(|change| change.revision)
            .unwrap_or(previous_revision);
        let snapshot = CliRuntimeReadinessSnapshot {
            revision,
            runtimes,
            models,
            mcp_readiness,
        };
        snapshots.insert(workspace_id.to_owned(), snapshot.clone());
        (snapshot, changed)
    }

    async fn replace_snapshot_if_generation(
        &self,
        workspace_id: &str,
        generation: u64,
        runtimes: Vec<RuntimeSummary>,
    ) -> Option<(CliRuntimeReadinessSnapshot, Vec<CliRuntimeReadinessChange>)> {
        // Hold the generation read guard until the snapshot write completes,
        // so a forced invalidation cannot interleave between the freshness
        // check and publication.
        let generations = self.cli_generations.read().await;
        if generations.get(workspace_id).copied() != Some(generation) {
            return None;
        }
        Some(self.replace_snapshot(workspace_id, runtimes).await)
    }

    async fn replace_probe_results_if_generation(
        &self,
        workspace_id: &str,
        generation: u64,
        runtimes: Vec<RuntimeSummary>,
        models: HashMap<String, CliRuntimeModelSnapshot>,
        mcp_readiness: HashMap<String, pioneer_protocol::CliMcpAdapterReadiness>,
    ) -> Option<(CliRuntimeReadinessSnapshot, Vec<CliRuntimeReadinessChange>)> {
        // Publish readiness, model catalogs, and MCP attestations as one
        // workspace value while the generation guard fences invalidation. A
        // reader can never combine artifacts from different probes.
        let generations = self.cli_generations.read().await;
        if generations.get(workspace_id).copied() != Some(generation) {
            return None;
        }
        Some(
            self.replace_snapshot_with_probe_artifacts(
                workspace_id,
                runtimes,
                models,
                mcp_readiness,
            )
            .await,
        )
    }

    async fn retain_workspaces(&self, workspace_ids: &HashSet<String>) {
        // Every operation that needs both maps uses generation -> snapshot.
        // Keep that order here as well; taking these guards in the opposite
        // order could deadlock with atomic invalidation or publication.
        let mut generations = self.cli_generations.write().await;
        let mut snapshots = self.cli_snapshots.write().await;
        let mut seed_locks = self.cli_seed_locks.write().await;
        generations.retain(|workspace_id, _| workspace_ids.contains(workspace_id));
        snapshots.retain(|workspace_id, _| workspace_ids.contains(workspace_id));
        seed_locks.retain(|workspace_id, _| workspace_ids.contains(workspace_id));
    }

    async fn cli_workspace_ids(&self) -> HashSet<String> {
        let mut workspace_ids = self
            .cli_snapshots
            .read()
            .await
            .keys()
            .cloned()
            .collect::<HashSet<_>>();
        workspace_ids.extend(self.cli_generations.read().await.keys().cloned());
        workspace_ids
    }
}

impl MessageProcessor {
    pub async fn start_provider_readiness_supervisor(self: &Arc<Self>) {
        let mut worker = self.provider_readiness.worker.lock().await;
        if worker.is_some() {
            return;
        }

        let (command_tx, command_rx) = mpsc::channel(1);
        let mut sender = self
            .provider_readiness
            .command_tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.provider_readiness
            .pending_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        *sender = Some(command_tx);
        drop(sender);
        let processor = Arc::downgrade(self);
        *worker = Some(tokio::spawn(async move {
            run_provider_readiness_worker(processor, command_rx).await;
        }));
        drop(worker);
        self.request_all_provider_warmup();
    }

    pub async fn shutdown_provider_readiness_supervisor(&self) {
        self.provider_readiness
            .command_tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.provider_readiness
            .pending_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        if let Some(mut worker) = self.provider_readiness.worker.lock().await.take()
            && tokio::time::timeout(Duration::from_secs(5), &mut worker)
                .await
                .is_err()
        {
            worker.abort();
            let _ = worker.await;
        }
    }

    pub(super) fn request_api_provider_warmup(&self, workspace_id: impl Into<String>) {
        self.provider_readiness
            .send(ProviderWarmupCommand::Workspace {
                workspace_id: workspace_id.into(),
                scope: ProviderWarmupScope::ApiOnly,
                force_if_running: true,
            });
    }

    pub(super) fn request_provider_warmup(&self, workspace_id: impl Into<String>) {
        self.provider_readiness
            .send(ProviderWarmupCommand::Workspace {
                workspace_id: workspace_id.into(),
                scope: ProviderWarmupScope::All,
                force_if_running: false,
            });
    }

    fn request_cli_runtime_warmup(&self, workspace_id: impl Into<String>) {
        self.provider_readiness
            .send(ProviderWarmupCommand::Workspace {
                workspace_id: workspace_id.into(),
                scope: ProviderWarmupScope::CliOnly,
                force_if_running: true,
            });
    }

    fn request_all_provider_warmup(&self) {
        self.provider_readiness
            .send(ProviderWarmupCommand::AllWorkspaces {
                scope: ProviderWarmupScope::All,
                force_if_running: false,
            });
    }

    fn request_all_cli_runtime_warmup(&self) {
        self.provider_readiness
            .send(ProviderWarmupCommand::AllWorkspaces {
                scope: ProviderWarmupScope::CliOnly,
                force_if_running: true,
            });
    }

    /// Fences the authoritative CLI snapshot before acknowledging a mutation,
    /// then schedules exactly one coalesced replacement probe.
    pub(super) async fn invalidate_and_request_cli_runtime_warmup(
        &self,
        workspace_id: impl Into<String>,
    ) {
        let workspace_id = workspace_id.into();
        if let Err(error) = self
            .invalidate_cli_runtime_readiness_snapshot(workspace_id.as_str())
            .await
        {
            warn!(
                workspace_id,
                error = %format!("{error:#}"),
                "failed to publish invalidated CLI readiness snapshot"
            );
        }
        self.request_cli_runtime_warmup(workspace_id);
    }

    /// Applies the same fail-closed fence to every known/active workspace for
    /// a Gateway-wide CLI configuration mutation.
    pub(super) async fn invalidate_and_request_all_cli_runtime_warmup(&self) {
        let mut workspace_ids = self.provider_readiness.cli_workspace_ids().await;
        match self.workspace_manager.list_workspaces().await {
            Ok(workspaces) => workspace_ids.extend(
                workspaces
                    .into_iter()
                    .filter(|workspace| workspace.is_active)
                    .map(|workspace| workspace.id),
            ),
            Err(error) => warn!(
                error = %error,
                "failed to list active workspaces while invalidating CLI readiness"
            ),
        }
        for workspace_id in workspace_ids {
            if let Err(error) = self
                .invalidate_cli_runtime_readiness_snapshot(workspace_id.as_str())
                .await
            {
                warn!(
                    workspace_id,
                    error = %format!("{error:#}"),
                    "failed to publish invalidated CLI readiness snapshot"
                );
            }
        }
        self.request_all_cli_runtime_warmup();
    }

    pub(super) async fn cli_runtime_readiness_snapshot_or_seed(
        &self,
        workspace_id: &str,
    ) -> anyhow::Result<CliRuntimeReadinessSnapshot> {
        if let Some(snapshot) = self.provider_readiness.snapshot(workspace_id).await {
            return Ok(snapshot);
        }

        // The listener can accept a discovery request in the brief interval
        // between Gateway readiness and the supervisor's first publication.
        // Coalesce those readers so a client burst performs at most one
        // configuration/secret lookup. This seed contains no live account,
        // process, network, or MCP probe; only the supervisor may perform
        // those operations.
        let seed_lock = self.provider_readiness.cli_seed_lock(workspace_id).await;
        let _seed_guard = seed_lock.lock().await;
        if let Some(snapshot) = self.provider_readiness.snapshot(workspace_id).await {
            return Ok(snapshot);
        }

        loop {
            let generation = self.provider_readiness.cli_generation(workspace_id).await;
            let runtimes = self.initializing_cli_runtime_snapshot(workspace_id).await?;
            if let Some(snapshot) = self
                .provider_readiness
                .insert_snapshot_if_absent_if_generation(workspace_id, generation, runtimes)
                .await
            {
                return Ok(snapshot);
            }
            // Configuration changed while the cheap seed was being built.
            // Retry against the new generation while still holding the
            // per-workspace seed lock; no provider process/network probe is
            // performed on this path.
        }
    }

    pub(super) async fn cli_runtime_probe_snapshot(
        &self,
        workspace_id: &str,
        runtime_id: &str,
    ) -> anyhow::Result<Option<CliRuntimeProbeSnapshot>> {
        let snapshot = self
            .cli_runtime_readiness_snapshot_or_seed(workspace_id)
            .await?;
        let Some(summary) = snapshot
            .runtimes
            .into_iter()
            .find(|runtime| runtime.runtime_id == runtime_id)
        else {
            return Ok(None);
        };
        Ok(Some(CliRuntimeProbeSnapshot {
            summary,
            models: snapshot.models.get(runtime_id).cloned(),
            mcp_readiness: snapshot.mcp_readiness.get(runtime_id).cloned(),
        }))
    }

    /// Test-only seam for message integration fixtures that replace the real
    /// CLI process manager with an in-memory recording runtime.
    ///
    /// Production readiness is always established by the supervisor. Tests
    /// must seed the same authoritative snapshot instead of bypassing the turn
    /// admission gate or relying on a client-triggered probe.
    #[cfg(test)]
    pub(super) async fn mark_cli_runtimes_ready_for_tests(
        &self,
        workspace_id: &str,
    ) -> anyhow::Result<()> {
        let mut runtimes = self.initializing_cli_runtime_snapshot(workspace_id).await?;
        let readiness_override = self.cli_mcp_readiness_override_for_tests();
        let mcp_supported = readiness_override
            .as_ref()
            .is_some_and(|readiness| readiness.supported);
        for runtime in &mut runtimes {
            if runtime.enabled {
                runtime.status = RuntimeStatus::Ready;
                runtime.capabilities.supports_mcp_tools = mcp_supported;
                runtime.diagnostics.retain(|diagnostic| {
                    diagnostic.code != "cli_runtime.initializing"
                        && (!mcp_supported || !diagnostic.code.starts_with("cli_runtime.mcp."))
                });
            }
        }
        let refreshed_at_unix_ms = chrono::Utc::now().timestamp_millis();
        let model_snapshots = runtimes
            .iter()
            .filter(|runtime| runtime.enabled)
            .map(|runtime| {
                (
                    runtime.runtime_id.clone(),
                    CliRuntimeModelSnapshot {
                        result: super::cli_runtime::RuntimeModelListResult {
                            models: Vec::new(),
                            diagnostics: Vec::new(),
                            error_message: None,
                        },
                        refreshed_at_unix_ms,
                    },
                )
            })
            .collect();
        let mcp_snapshots = readiness_override.map_or_else(HashMap::new, |readiness| {
            runtimes
                .iter()
                .filter(|runtime| runtime.enabled)
                .map(|runtime| (runtime.runtime_id.clone(), readiness.clone()))
                .collect()
        });
        let generation = self.provider_readiness.cli_generation(workspace_id).await;
        self.provider_readiness
            .replace_probe_results_if_generation(
                workspace_id,
                generation,
                runtimes,
                model_snapshots,
                mcp_snapshots,
            )
            .await
            .ok_or_else(|| anyhow::anyhow!("test CLI readiness generation was superseded"))?;
        Ok(())
    }

    async fn perform_provider_warmup(
        &self,
        workspace_id: &str,
        scope: ProviderWarmupScope,
        provider_concurrency: &Arc<Semaphore>,
        trace: &GatewayProviderWarmupTrace,
    ) -> anyhow::Result<ProviderWarmupExecution> {
        let workspace_stage = trace.stage(GatewayProviderWarmupStage::WorkspaceLoad);
        let workspace_id = match self
            .workspace_manager
            .validate_workspace_id(workspace_id)
            .await
        {
            Ok(workspace_id) => workspace_id,
            Err(
                crate::workspace::WorkspaceError::WorkspaceNotFound(_)
                | crate::workspace::WorkspaceError::WorkspaceInactive(_),
            ) => {
                // A workspace can be retired while a coalesced rerun is
                // waiting behind an in-flight probe. That is a terminal
                // cancellation for this work item, not a provider failure and
                // must never create an endless retry entry.
                workspace_stage.cancel();
                return Ok(ProviderWarmupExecution {
                    retry_scope: None,
                    superseded: true,
                });
            }
            Err(error) => return Err(anyhow::anyhow!(error.to_string())),
        };
        workspace_stage.succeed();
        let cli_generation = if scope.includes_cli() {
            Some(
                self.provider_readiness
                    .cli_generation(workspace_id.as_str())
                    .await,
            )
        } else {
            None
        };

        let (api_ready, cli_outcome) = match (scope.includes_api(), scope.includes_cli()) {
            (true, true) => {
                let cli_generation = cli_generation.ok_or_else(|| {
                    anyhow::anyhow!("CLI warm-up started without a configuration generation")
                })?;
                let (api_result, cli_result) = tokio::join!(
                    self.perform_api_provider_warmup(
                        workspace_id.as_str(),
                        &trace,
                        provider_concurrency,
                    ),
                    self.perform_cli_runtime_warmup(
                        workspace_id.as_str(),
                        &trace,
                        provider_concurrency,
                        cli_generation,
                    ),
                );
                (api_result?, Some(cli_result?))
            }
            (true, false) => {
                let ready = self
                    .perform_api_provider_warmup(
                        workspace_id.as_str(),
                        &trace,
                        provider_concurrency,
                    )
                    .await?;
                (ready, None)
            }
            (false, true) => {
                let cli_generation = cli_generation.ok_or_else(|| {
                    anyhow::anyhow!("CLI warm-up started without a configuration generation")
                })?;
                let ready = self
                    .perform_cli_runtime_warmup(
                        workspace_id.as_str(),
                        &trace,
                        provider_concurrency,
                        cli_generation,
                    )
                    .await?;
                (true, Some(ready))
            }
            (false, false) => {
                return Err(anyhow::anyhow!(
                    "provider warm-up scope did not include a provider kind"
                ));
            }
        };
        Ok(ProviderWarmupExecution {
            retry_scope: ProviderWarmupScope::from_retry_needs(
                scope.includes_api() && !api_ready,
                matches!(cli_outcome, Some(CliRuntimeWarmupOutcome::Retry)),
            ),
            superseded: matches!(cli_outcome, Some(CliRuntimeWarmupOutcome::Superseded)),
        })
    }

    async fn perform_api_provider_warmup(
        &self,
        workspace_id: &str,
        trace: &GatewayProviderWarmupTrace,
        provider_concurrency: &Arc<Semaphore>,
    ) -> anyhow::Result<bool> {
        let api_catalog_stage = trace.stage(GatewayProviderWarmupStage::ApiCatalogLoad);
        let mut provider_names = BTreeSet::from(["local".to_owned()]);
        provider_names.extend(
            self.gateway_secrets
                .list_configured_workspace_provider_names(workspace_id)?,
        );
        provider_names.extend(
            self.gateway_secrets
                .list_workspace_provider_proxies(workspace_id)?
                .into_iter()
                .map(|(provider, _)| provider),
        );
        api_catalog_stage.succeed();

        let api_warmup_stage = trace.stage(GatewayProviderWarmupStage::ApiInstancesWarmup);
        let api_warmups = provider_names.into_iter().map(|provider_name| {
            let provider_concurrency = provider_concurrency.clone();
            async move {
                let _permit = provider_concurrency
                    .acquire_owned()
                    .await
                    .map_err(|error| {
                        (
                            "unknown".to_owned(),
                            provider_name.clone(),
                            error.to_string(),
                        )
                    })?;
                let provider = self
                    .provider_registry
                    .get_or_create_for_workspace(workspace_id, provider_name.as_str());
                match provider {
                    Ok(provider) => {
                        let provider_kind = provider.name().to_owned();
                        let provider_stage = trace.api_provider_stage(
                            GatewayProviderWarmupStage::ApiInstanceWarmup,
                            provider_kind.clone(),
                        );
                        match tokio::time::timeout(
                            PROVIDER_READINESS_PROBE_TIMEOUT,
                            provider.warmup(),
                        )
                        .await
                        {
                            Ok(Ok(outcome)) => {
                                provider_stage.succeed();
                                Ok((provider_kind, outcome))
                            }
                            Ok(Err(error)) => {
                                Err((provider_kind, provider_name, format!("{error:#}")))
                            }
                            Err(_) => Err((
                                provider_kind,
                                provider_name,
                                "provider warm-up timed out".to_owned(),
                            )),
                        }
                    }
                    Err(error) => Err(("unknown".to_owned(), provider_name, format!("{error:#}"))),
                }
            }
        });
        let mut all_ready = true;
        for result in join_all(api_warmups).await {
            match result {
                Ok((provider_kind, pioneer_provider::ProviderWarmupOutcome::Completed)) => {
                    trace.record_api_readiness(provider_kind, GatewayProviderReadinessState::Ready);
                }
                Ok((provider_kind, pioneer_provider::ProviderWarmupOutcome::NotSupported)) => {
                    trace.record_api_readiness(
                        provider_kind,
                        GatewayProviderReadinessState::Unverified,
                    );
                }
                Err((provider_kind, provider, error)) => {
                    all_ready = false;
                    trace.record_api_readiness(provider_kind, GatewayProviderReadinessState::Error);
                    warn!(provider, error, "API provider warm-up did not complete");
                }
            }
        }
        api_warmup_stage.succeed();
        Ok(all_ready)
    }

    async fn perform_cli_runtime_warmup(
        &self,
        workspace_id: &str,
        trace: &GatewayProviderWarmupTrace,
        provider_concurrency: &Arc<Semaphore>,
        generation: u64,
    ) -> anyhow::Result<CliRuntimeWarmupOutcome> {
        let cli_catalog_stage = trace.stage(GatewayProviderWarmupStage::CliCatalogLoad);
        let instances = self.load_cli_runtime_instances()?;
        cli_catalog_stage.succeed();

        let probe_results = join_all(instances.into_iter().map(|instance| {
            let provider_concurrency = provider_concurrency.clone();
            async move {
                let runtime_kind = instance.kind;
                let fallback_instance = instance.clone();
                let result = async {
                    let _permit = provider_concurrency
                        .acquire_owned()
                        .await
                        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                    tokio::time::timeout(CLI_RUNTIME_READINESS_PROBE_TIMEOUT, async {
                        let live_readiness = self
                            .cli_runtime_live_summary_from_instance_with_trace(
                                workspace_id,
                                instance.clone(),
                                Some(trace),
                            )
                            .await?;
                        let mut summary = live_readiness.summary;
                        let models = if matches!(summary.status, RuntimeStatus::Ready) {
                            let result = self
                                .cli_runtime_model_list_from_instance_with_trace(
                                    &instance,
                                    summary.proxy_url.as_deref(),
                                    Some(trace),
                                )
                                .await;
                            let refreshed_at_unix_ms = chrono::Utc::now().timestamp_millis();
                            summary.models_refreshed_at_unix_ms = Some(refreshed_at_unix_ms);
                            if let Some(error) = result.error_message.as_deref() {
                                summary.status = RuntimeStatus::Degraded {
                                    message: "runtime model catalog is unavailable".to_owned(),
                                };
                                summary.diagnostics.extend(result.diagnostics.clone());
                                warn!(
                                    runtime_kind = ?runtime_kind,
                                    error,
                                    "CLI runtime model catalog is not ready"
                                );
                            }
                            Some(CliRuntimeModelSnapshot {
                                result,
                                refreshed_at_unix_ms,
                            })
                        } else {
                            None
                        };
                        Ok::<_, anyhow::Error>(CliRuntimeProbeResult {
                            summary,
                            models,
                            mcp_readiness: live_readiness.mcp_readiness,
                        })
                    })
                    .await
                    .map_err(|_| anyhow::anyhow!("CLI runtime readiness check timed out"))?
                }
                .await;
                match result {
                    Ok(result) => result,
                    Err(error) => {
                        warn!(
                            runtime_kind = ?runtime_kind,
                            error = %format!("{error:#}"),
                            "CLI runtime readiness check failed"
                        );
                        CliRuntimeProbeResult {
                            summary: cli_runtime_error_summary(fallback_instance),
                            models: None,
                            mcp_readiness: None,
                        }
                    }
                }
            }
        }))
        .await;
        let mut runtimes = probe_results
            .iter()
            .map(|result| result.summary.clone())
            .collect::<Vec<_>>();
        let model_snapshots = probe_results
            .iter()
            .filter_map(|result| {
                result
                    .models
                    .clone()
                    .map(|models| (result.summary.runtime_id.clone(), models))
            })
            .collect::<HashMap<_, _>>();
        let mcp_snapshots = probe_results
            .into_iter()
            .filter_map(|result| {
                result
                    .mcp_readiness
                    .map(|readiness| (result.summary.runtime_id, readiness))
            })
            .collect::<HashMap<_, _>>();
        super::cli_runtime::sort_cli_runtime_summary_display_order(runtimes.as_mut_slice());
        let Some((snapshot, changed)) = self
            .provider_readiness
            .replace_probe_results_if_generation(
                workspace_id,
                generation,
                runtimes,
                model_snapshots,
                mcp_snapshots,
            )
            .await
        else {
            // Configuration changed while these probes were in flight. The
            // forced command has already queued a rerun; never publish stale
            // process/account/MCP/model evidence over the invalidated snapshot.
            return Ok(CliRuntimeWarmupOutcome::Superseded);
        };
        let all_ready = snapshot.runtimes.iter().all(|runtime| {
            !runtime.enabled
                || telemetry_cli_runtime_readiness(runtime) == GatewayProviderReadinessState::Ready
        });
        for runtime in &snapshot.runtimes {
            trace.record_cli_readiness(
                telemetry_cli_runtime_kind(runtime.kind),
                telemetry_cli_runtime_readiness(runtime),
            );
        }

        let publish_stage = trace.stage(GatewayProviderWarmupStage::CliSnapshotPublish);
        self.publish_cli_runtime_readiness_changes(workspace_id, changed)
            .await;
        publish_stage.succeed();
        Ok(if all_ready {
            CliRuntimeWarmupOutcome::Ready
        } else {
            CliRuntimeWarmupOutcome::Retry
        })
    }

    async fn invalidate_cli_runtime_readiness_snapshot(
        &self,
        workspace_id: &str,
    ) -> anyhow::Result<()> {
        let (generation, invalidated_changes) = self
            .provider_readiness
            .invalidate_snapshot(workspace_id)
            .await;
        self.publish_cli_runtime_readiness_changes(workspace_id, invalidated_changes)
            .await;
        let runtimes = match self.initializing_cli_runtime_snapshot(workspace_id).await {
            Ok(runtimes) => runtimes,
            Err(error) => {
                if let Some(snapshot) = self.provider_readiness.snapshot(workspace_id).await {
                    let runtimes = snapshot
                        .runtimes
                        .into_iter()
                        .map(cli_runtime_readiness_error_summary)
                        .collect();
                    let changes = self
                        .provider_readiness
                        .replace_snapshot_if_generation(workspace_id, generation, runtimes)
                        .await
                        .map(|(_, changes)| changes)
                        .unwrap_or_default();
                    self.publish_cli_runtime_readiness_changes(workspace_id, changes)
                        .await;
                }
                return Err(error);
            }
        };
        let changes = self
            .provider_readiness
            .replace_snapshot_if_generation(workspace_id, generation, runtimes)
            .await
            .map(|(_, changes)| changes)
            .unwrap_or_default();
        self.publish_cli_runtime_readiness_changes(workspace_id, changes)
            .await;
        Ok(())
    }

    async fn initializing_cli_runtime_snapshot(
        &self,
        workspace_id: &str,
    ) -> anyhow::Result<Vec<RuntimeSummary>> {
        let instances = self.load_cli_runtime_instances()?;
        self.initializing_cli_runtime_snapshot_from_instances(workspace_id, instances.as_slice())
            .await
    }

    async fn initializing_cli_runtime_snapshot_from_instances(
        &self,
        workspace_id: &str,
        instances: &[pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig],
    ) -> anyhow::Result<Vec<RuntimeSummary>> {
        let mut summaries = join_all(instances.iter().cloned().map(|instance| async move {
            match self
                .prepare_cli_runtime_proxy_url(workspace_id, instance.id.as_str())
                .await
            {
                Ok(proxy_url) => {
                    super::cli_runtime::cli_runtime_summary_from_instance(instance, proxy_url)
                }
                Err(error) => {
                    warn!(
                        runtime_kind = ?instance.kind,
                        error = %format!("{error:#}"),
                        "failed to prepare CLI runtime while seeding readiness snapshot"
                    );
                    cli_runtime_error_summary(instance)
                }
            }
        }))
        .await;
        super::cli_runtime::sort_cli_runtime_summary_display_order(summaries.as_mut_slice());
        Ok(summaries)
    }

    async fn publish_cli_runtime_readiness_changes(
        &self,
        workspace_id: &str,
        changes: Vec<CliRuntimeReadinessChange>,
    ) {
        for change in changes {
            self.send_cli_runtime_status_changed_notification(
                workspace_id,
                change.revision,
                change.runtime,
                change.removed,
            )
            .await;
        }
    }
}

fn cli_runtime_error_summary(
    instance: pioneer_config::EffectiveGatewayCliAgentRuntimeInstanceConfig,
) -> RuntimeSummary {
    cli_runtime_readiness_error_summary(super::cli_runtime::cli_runtime_summary_from_instance(
        instance, None,
    ))
}

fn cli_runtime_readiness_error_summary(mut summary: RuntimeSummary) -> RuntimeSummary {
    if !summary.enabled {
        return summary;
    }
    summary.status = RuntimeStatus::Error {
        message: "Gateway could not determine runtime readiness".to_owned(),
    };
    summary
        .diagnostics
        .retain(|diagnostic| diagnostic.code != "cli_runtime.initializing");
    summary
        .diagnostics
        .push(pioneer_protocol::RuntimeDiagnostic {
            level: pioneer_protocol::RuntimeDiagnosticLevel::Error,
            code: "cli_runtime.readiness_failed".to_owned(),
            message: "Gateway could not determine runtime readiness".to_owned(),
        });
    summary
}

fn telemetry_cli_runtime_kind(kind: CLIAgentRuntimeKind) -> GatewayCliRuntimeKind {
    match kind {
        CLIAgentRuntimeKind::Codex => GatewayCliRuntimeKind::Codex,
        CLIAgentRuntimeKind::Claude => GatewayCliRuntimeKind::Claude,
    }
}

fn telemetry_cli_runtime_readiness(runtime: &RuntimeSummary) -> GatewayProviderReadinessState {
    match &runtime.status {
        RuntimeStatus::Disabled => GatewayProviderReadinessState::Disabled,
        RuntimeStatus::MissingBinary { .. } => GatewayProviderReadinessState::MissingBinary,
        RuntimeStatus::SpawnFailed { .. } => GatewayProviderReadinessState::SpawnFailed,
        RuntimeStatus::Initializing => GatewayProviderReadinessState::Initializing,
        RuntimeStatus::NeedsAuth => GatewayProviderReadinessState::NeedsAuth,
        // Account/process readiness is enough to show and use the provider,
        // but the supervisor keeps retrying a failed local MCP attestation in
        // the background until the complete provider integration is ready.
        RuntimeStatus::Ready if runtime.capabilities.supports_mcp_tools => {
            GatewayProviderReadinessState::Ready
        }
        RuntimeStatus::Ready => GatewayProviderReadinessState::Degraded,
        RuntimeStatus::Degraded { .. } => GatewayProviderReadinessState::Degraded,
        RuntimeStatus::UnsupportedVersion { .. } => {
            GatewayProviderReadinessState::UnsupportedVersion
        }
        RuntimeStatus::Error { .. } => GatewayProviderReadinessState::Error,
    }
}

/// Owns the bounded task set and all singleflight state for provider probes.
///
/// Keeping these values together prevents scheduling call sites from passing
/// partially matching maps or concurrency guards. The worker below owns one
/// scheduler for its entire lifetime.
struct ProviderProbeScheduler {
    probes: JoinSet<ProviderWarmupTaskCompletion>,
    task_workspaces: HashMap<tokio::task::Id, String>,
    in_flight: HashMap<String, ProviderWarmupScope>,
    rerun: HashMap<String, ProviderWarmupScope>,
    processor: Weak<MessageProcessor>,
    workspace_concurrency: Arc<Semaphore>,
    provider_concurrency: Arc<Semaphore>,
}

impl ProviderProbeScheduler {
    fn new(processor: Weak<MessageProcessor>) -> Self {
        Self {
            probes: JoinSet::new(),
            task_workspaces: HashMap::new(),
            in_flight: HashMap::new(),
            rerun: HashMap::new(),
            processor,
            workspace_concurrency: Arc::new(Semaphore::new(MAX_CONCURRENT_WORKSPACE_WARMUPS)),
            provider_concurrency: Arc::new(Semaphore::new(MAX_CONCURRENT_PROVIDER_PROBES)),
        }
    }

    async fn schedule_all(
        &mut self,
        scope: ProviderWarmupScope,
        force_if_running: bool,
        only: Option<&HashSet<String>>,
    ) -> Option<HashSet<String>> {
        let message_processor = self.processor.upgrade()?;
        let workspaces = match message_processor.workspace_manager.list_workspaces().await {
            Ok(workspaces) => workspaces,
            Err(error) => {
                warn!(error = %error, "failed to list workspaces for provider warm-up");
                return None;
            }
        };
        let active_workspaces = workspaces
            .into_iter()
            .filter(|workspace| workspace.is_active)
            .collect::<Vec<_>>();
        let active_workspace_ids = active_workspaces
            .iter()
            .map(|workspace| workspace.id.clone())
            .collect::<HashSet<_>>();
        message_processor
            .provider_readiness
            .retain_workspaces(&active_workspace_ids)
            .await;
        drop(message_processor);

        for workspace in active_workspaces {
            if only.is_some_and(|only| !only.contains(workspace.id.as_str())) {
                continue;
            }
            self.schedule_workspace(workspace.id, scope, force_if_running);
        }
        Some(active_workspace_ids)
    }

    fn schedule_workspace(
        &mut self,
        workspace_id: String,
        scope: ProviderWarmupScope,
        force_if_running: bool,
    ) -> bool {
        if !register_workspace_probe(
            &mut self.in_flight,
            &mut self.rerun,
            workspace_id.as_str(),
            scope,
            force_if_running,
        ) {
            return false;
        }

        let processor = self.processor.clone();
        let workspace_concurrency = self.workspace_concurrency.clone();
        let provider_concurrency = self.provider_concurrency.clone();
        let task_workspace_id = workspace_id.clone();
        let abort_handle = self.probes.spawn(async move {
            let trace = GatewayProviderWarmupTrace::start(scope.telemetry_scope());
            let result = AssertUnwindSafe(async {
                let queue_stage = trace.stage(GatewayProviderWarmupStage::SchedulerQueueWait);
                let _permit = workspace_concurrency
                    .acquire_owned()
                    .await
                    .map_err(|_| anyhow::anyhow!("provider warm-up concurrency limiter closed"))?;
                queue_stage.succeed();
                if let Some(processor) = processor.upgrade() {
                    processor
                        .perform_provider_warmup(
                            workspace_id.as_str(),
                            scope,
                            &provider_concurrency,
                            &trace,
                        )
                        .await
                } else {
                    Err(anyhow::anyhow!("Gateway message processor stopped"))
                }
            })
            .catch_unwind()
            .await;
            let result = match result {
                Ok(Ok(execution)) => {
                    if execution.superseded {
                        trace.finish_superseded();
                    } else {
                        trace.finish_success();
                    }
                    Ok(Ok(execution.retry_scope))
                }
                Ok(Err(error)) => {
                    trace.finish_failure();
                    Ok(Err(error))
                }
                Err(panic) => {
                    trace.finish_failure();
                    Err(panic)
                }
            };
            (workspace_id, scope, result)
        });
        self.task_workspaces
            .insert(abort_handle.id(), task_workspace_id);
        true
    }

    async fn shutdown(&mut self) {
        self.probes.abort_all();
        while self.probes.join_next().await.is_some() {}
        self.task_workspaces.clear();
        self.in_flight.clear();
        self.rerun.clear();
    }
}

async fn run_provider_readiness_worker(
    processor: Weak<MessageProcessor>,
    mut command_rx: mpsc::Receiver<()>,
) {
    let mut scheduler = ProviderProbeScheduler::new(processor.clone());
    let mut pending_retries = HashMap::<String, ProviderWarmupScope>::new();
    let mut retry_schedule = HashMap::<String, ProviderRetrySchedule>::new();
    let mut pending_all_workspaces_retry = None::<ProviderWarmupScope>;
    let mut all_workspaces_retry_schedule = ProviderRetrySchedule::default();
    let mut retry = tokio::time::interval_at(
        tokio::time::Instant::now() + PROVIDER_READINESS_RETRY_INTERVAL,
        PROVIDER_READINESS_RETRY_INTERVAL,
    );
    retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut reconcile = tokio::time::interval_at(
        tokio::time::Instant::now() + PROVIDER_READINESS_RECONCILE_INTERVAL,
        PROVIDER_READINESS_RECONCILE_INTERVAL,
    );
    reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            wake = command_rx.recv() => {
                let Some(()) = wake else { break; };
                let Some(message_processor) = processor.upgrade() else { break; };
                let commands = message_processor
                    .provider_readiness
                    .pending_commands
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .drain();
                drop(message_processor);
                for command in commands {
                    match command {
                    ProviderWarmupCommand::Workspace { workspace_id, scope, force_if_running } => {
                        if force_if_running {
                            retry_schedule.remove(workspace_id.as_str());
                        }
                        scheduler.schedule_workspace(workspace_id, scope, force_if_running);
                    }
                    ProviderWarmupCommand::AllWorkspaces { scope, force_if_running } => {
                        if force_if_running {
                            retry_schedule.clear();
                            all_workspaces_retry_schedule = ProviderRetrySchedule::default();
                        }
                        if let Some(active_workspace_ids) = scheduler
                            .schedule_all(scope, force_if_running, None)
                            .await
                        {
                            pending_all_workspaces_retry = pending_all_workspaces_retry
                                .and_then(|pending| pending.subtract(scope));
                            pending_retries.retain(|workspace_id, _| {
                                active_workspace_ids.contains(workspace_id)
                            });
                            retry_schedule.retain(|workspace_id, _| {
                                active_workspace_ids.contains(workspace_id)
                            });
                        } else {
                            merge_optional_scope(&mut pending_all_workspaces_retry, scope);
                            if all_workspaces_retry_schedule.next_attempt.is_none() {
                                all_workspaces_retry_schedule
                                    .record_attempt(tokio::time::Instant::now());
                            }
                        }
                    }
                    }
                }
            }
            result = scheduler.probes.join_next_with_id(), if !scheduler.probes.is_empty() => {
                let Some(result) = result else { continue; };
                let (workspace_id, requested_scope, outcome) = match result {
                    Ok((task_id, (workspace_id, requested_scope, outcome))) => {
                        let registered_workspace = scheduler.task_workspaces.remove(&task_id);
                        debug_assert_eq!(registered_workspace.as_deref(), Some(workspace_id.as_str()));
                        (workspace_id, requested_scope, outcome)
                    }
                    Err(join_error) => {
                        // The task body catches provider panics itself, so a
                        // JoinError normally means runtime-driven cancellation.
                        // Recover the workspace from the task id, clear its
                        // singleflight slot below, and put the requested scope
                        // back through the bounded retry path. Otherwise one
                        // cancelled task could block reconciliation forever.
                        let task_id = join_error.id();
                        let Some(workspace_id) = scheduler.task_workspaces.remove(&task_id) else {
                            warn!(task_id = ?task_id, error = %join_error, "unregistered provider warm-up task stopped");
                            continue;
                        };
                        let Some(requested_scope) = scheduler.in_flight.get(workspace_id.as_str()).copied() else {
                            warn!(workspace_id, task_id = ?task_id, error = %join_error, "provider warm-up task stopped without an in-flight registration");
                            continue;
                        };
                        let outcome = Ok(Err(anyhow::anyhow!(
                            "provider warm-up task stopped unexpectedly: {join_error}"
                        )));
                        (workspace_id, requested_scope, outcome)
                    }
                };
                scheduler.in_flight.remove(workspace_id.as_str());
                let retry_scope = match outcome {
                    Ok(Ok(retry_scope)) => retry_scope,
                    Ok(Err(error)) => {
                        warn!(
                        workspace_id,
                        error = %format!("{error:#}"),
                        "Gateway provider warm-up failed"
                        );
                        Some(requested_scope)
                    }
                    Err(_) => {
                        warn!(
                            workspace_id,
                            "Gateway provider warm-up panicked; workspace scheduling recovered"
                        );
                        Some(requested_scope)
                    }
                };
                complete_retry_scope(
                    &mut pending_retries,
                    workspace_id.as_str(),
                    requested_scope,
                    retry_scope,
                );
                if pending_retries.contains_key(workspace_id.as_str()) {
                    retry_schedule
                        .entry(workspace_id.clone())
                        .or_default()
                        .ensure_scheduled(tokio::time::Instant::now());
                } else {
                    retry_schedule.remove(workspace_id.as_str());
                }
                if let Some(scope) = scheduler.rerun.remove(workspace_id.as_str()) {
                    scheduler.schedule_workspace(workspace_id, scope, false);
                }
            }
            _ = retry.tick() => {
                let now = tokio::time::Instant::now();
                if let Some(scope) = pending_all_workspaces_retry
                    && all_workspaces_retry_schedule
                        .next_attempt
                        .is_none_or(|next_attempt| next_attempt <= now)
                {
                        if let Some(active_workspace_ids) =
                            scheduler.schedule_all(scope, false, None).await
                        {
                            pending_all_workspaces_retry = None;
                            all_workspaces_retry_schedule = ProviderRetrySchedule::default();
                            pending_retries.retain(|workspace_id, _| {
                                active_workspace_ids.contains(workspace_id)
                            });
                            retry_schedule.retain(|workspace_id, _| {
                                active_workspace_ids.contains(workspace_id)
                            });
                        } else {
                            all_workspaces_retry_schedule.record_attempt(now);
                        }
                }
                let retry_workspaces = pending_retries
                    .iter()
                    .map(|(workspace_id, scope)| (workspace_id.clone(), *scope))
                    .collect::<Vec<_>>();
                retry_schedule.retain(|workspace_id, _| {
                    pending_retries.contains_key(workspace_id)
                });
                for (workspace_id, scope) in retry_workspaces {
                    let schedule = retry_schedule.entry(workspace_id.clone()).or_default();
                    if schedule.next_attempt.is_some_and(|next_attempt| next_attempt > now) {
                        continue;
                    }
                    let scheduled = scheduler.schedule_workspace(workspace_id, scope, false);
                    if scheduled {
                        schedule.record_attempt(now);
                    }
                }
            }
            _ = reconcile.tick() => {
                if let Some(active_workspace_ids) = scheduler
                    .schedule_all(ProviderWarmupScope::All, false, None)
                    .await
                {
                    pending_retries.retain(|workspace_id, _| {
                        active_workspace_ids.contains(workspace_id)
                    });
                    retry_schedule.retain(|workspace_id, _| {
                        active_workspace_ids.contains(workspace_id)
                    });
                } else {
                    merge_optional_scope(
                        &mut pending_all_workspaces_retry,
                        ProviderWarmupScope::All,
                    );
                    all_workspaces_retry_schedule.ensure_scheduled(tokio::time::Instant::now());
                }
            }
        }
    }
    scheduler.shutdown().await;
}

#[derive(Clone, Copy, Debug, Default)]
struct ProviderRetrySchedule {
    attempts: u32,
    next_attempt: Option<tokio::time::Instant>,
}

impl ProviderRetrySchedule {
    fn ensure_scheduled(&mut self, now: tokio::time::Instant) {
        if self.next_attempt.is_none() {
            self.record_attempt(now);
        }
    }

    fn record_attempt(&mut self, now: tokio::time::Instant) {
        let delay = provider_retry_delay(self.attempts);
        self.attempts = self.attempts.saturating_add(1);
        self.next_attempt = Some(now + delay);
    }
}

fn merge_optional_scope(target: &mut Option<ProviderWarmupScope>, scope: ProviderWarmupScope) {
    *target = Some(target.map_or(scope, |pending| pending.merge(scope)));
}

fn complete_retry_scope(
    pending_retries: &mut HashMap<String, ProviderWarmupScope>,
    workspace_id: &str,
    completed_scope: ProviderWarmupScope,
    retry_scope: Option<ProviderWarmupScope>,
) {
    let remaining = pending_retries
        .remove(workspace_id)
        .and_then(|pending| pending.subtract(completed_scope));
    let next = match (remaining, retry_scope) {
        (Some(remaining), Some(retry)) => Some(remaining.merge(retry)),
        (remaining, retry) => remaining.or(retry),
    };
    if let Some(next) = next {
        pending_retries.insert(workspace_id.to_owned(), next);
    }
}

fn provider_retry_delay(attempts: u32) -> Duration {
    let exponent = attempts.min(4);
    PROVIDER_READINESS_RETRY_INTERVAL
        .saturating_mul(1_u32 << exponent)
        .min(PROVIDER_READINESS_RETRY_MAX_DELAY)
}

fn register_workspace_probe(
    in_flight: &mut HashMap<String, ProviderWarmupScope>,
    rerun: &mut HashMap<String, ProviderWarmupScope>,
    workspace_id: &str,
    scope: ProviderWarmupScope,
    force_if_running: bool,
) -> bool {
    let Some(running_scope) = in_flight.get(workspace_id).copied() else {
        in_flight.insert(workspace_id.to_owned(), scope);
        return true;
    };
    if force_if_running || running_scope.merge(scope) != running_scope {
        rerun
            .entry(workspace_id.to_owned())
            .and_modify(|pending| *pending = pending.merge(scope))
            .or_insert(scope);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime_summary(runtime_id: &str, status: RuntimeStatus) -> RuntimeSummary {
        RuntimeSummary {
            runtime_id: runtime_id.to_owned(),
            kind: CLIAgentRuntimeKind::Codex,
            display_name: runtime_id.to_owned(),
            enabled: true,
            status,
            capabilities: Default::default(),
            account: None,
            version: None,
            binary_path: None,
            home_path: None,
            shadow_home_path: None,
            proxy_url: None,
            debug_native_events_enabled: false,
            models_refreshed_at_unix_ms: None,
            diagnostics: Vec::new(),
            recent_stderr: Vec::new(),
        }
    }

    fn model_snapshot(model_id: &str) -> CliRuntimeModelSnapshot {
        CliRuntimeModelSnapshot {
            result: super::super::cli_runtime::RuntimeModelListResult {
                models: vec![RuntimeModelInfo {
                    id: model_id.to_owned(),
                    name: Some(model_id.to_owned()),
                    description: None,
                    family: None,
                    is_custom: false,
                    active: Some(true),
                    effort_options: Vec::new(),
                    input_modalities: Vec::new(),
                    output_modalities: Vec::new(),
                    supports_reasoning: None,
                    supports_vision: None,
                    max_input_tokens: None,
                    max_output_tokens: None,
                }],
                diagnostics: Vec::new(),
                error_message: None,
            },
            refreshed_at_unix_ms: 1,
        }
    }

    fn mcp_readiness() -> pioneer_protocol::CliMcpAdapterReadiness {
        pioneer_protocol::CliMcpAdapterReadiness {
            supported: true,
            injection: pioneer_protocol::CliMcpInjectionKind::CodexManagedStdioMcp,
            projection_update:
                pioneer_protocol::CliMcpProjectionUpdateKind::CodexRestartAppServerResumeThread,
            strict_isolation: true,
            contract_fingerprint: "c".repeat(64),
            local_executable_fingerprint: "e".repeat(64),
            provider_version: Some("test".to_owned()),
            max_tools: 1_024,
            max_schema_bytes: 8 * 1_024 * 1_024,
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn command_inbox_coalesces_internal_bursts_without_queue_growth() {
        let mut inbox = ProviderWarmupInbox::default();
        for _ in 0..100 {
            inbox.push(ProviderWarmupCommand::Workspace {
                workspace_id: "ws_default".to_owned(),
                scope: ProviderWarmupScope::CliOnly,
                force_if_running: false,
            });
        }
        inbox.push(ProviderWarmupCommand::Workspace {
            workspace_id: "ws_default".to_owned(),
            scope: ProviderWarmupScope::ApiOnly,
            force_if_running: true,
        });

        let commands = inbox.drain();
        assert_eq!(commands.len(), 1);
        assert!(matches!(
            &commands[0],
            ProviderWarmupCommand::Workspace {
                workspace_id,
                scope: ProviderWarmupScope::All,
                force_if_running: true,
            } if workspace_id == "ws_default"
        ));
    }

    #[test]
    fn ready_cli_runtime_with_incomplete_mcp_attestation_remains_retryable() {
        let mut runtime = runtime_summary("codex", RuntimeStatus::Ready);
        assert_eq!(
            telemetry_cli_runtime_readiness(&runtime),
            GatewayProviderReadinessState::Degraded
        );

        runtime.capabilities.supports_mcp_tools = true;
        assert_eq!(
            telemetry_cli_runtime_readiness(&runtime),
            GatewayProviderReadinessState::Ready
        );
    }

    #[tokio::test]
    async fn late_seed_cannot_replace_a_completed_readiness_snapshot() {
        let supervisor = ProviderReadinessSupervisor::default();
        let generation = supervisor.cli_generation("ws_default").await;
        supervisor
            .insert_snapshot_if_absent_if_generation(
                "ws_default",
                generation,
                vec![runtime_summary("codex", RuntimeStatus::Initializing)],
            )
            .await
            .expect("initial seed");
        let (ready, _) = supervisor
            .replace_snapshot(
                "ws_default",
                vec![runtime_summary("codex", RuntimeStatus::Ready)],
            )
            .await;
        let late_seed = supervisor
            .insert_snapshot_if_absent_if_generation(
                "ws_default",
                generation,
                vec![runtime_summary("codex", RuntimeStatus::Initializing)],
            )
            .await
            .expect("same-generation seed");

        assert_eq!(ready.revision, 2);
        assert_eq!(late_seed, ready);
    }

    #[tokio::test]
    async fn snapshot_revision_reports_runtime_removal_explicitly() {
        let supervisor = ProviderReadinessSupervisor::default();
        let generation = supervisor.cli_generation("ws_default").await;
        supervisor
            .insert_snapshot_if_absent_if_generation(
                "ws_default",
                generation,
                vec![runtime_summary("claude", RuntimeStatus::Ready)],
            )
            .await
            .expect("initial seed");

        let (snapshot, changes) = supervisor.replace_snapshot("ws_default", Vec::new()).await;

        assert_eq!(snapshot.revision, 2);
        assert!(snapshot.runtimes.is_empty());
        assert_eq!(changes.len(), 1);
        assert!(changes[0].removed);
        assert_eq!(changes[0].revision, 2);
        assert_eq!(changes[0].runtime.runtime_id, "claude");
    }

    #[tokio::test]
    async fn every_runtime_delta_has_a_distinct_contiguous_revision() {
        let supervisor = ProviderReadinessSupervisor::default();
        let generation = supervisor.cli_generation("ws_default").await;
        supervisor
            .insert_snapshot_if_absent_if_generation(
                "ws_default",
                generation,
                vec![
                    runtime_summary("codex", RuntimeStatus::Initializing),
                    runtime_summary("claude", RuntimeStatus::Initializing),
                ],
            )
            .await
            .expect("initial seed");

        let (snapshot, changes) = supervisor
            .replace_snapshot(
                "ws_default",
                vec![
                    runtime_summary("codex", RuntimeStatus::Ready),
                    runtime_summary("claude", RuntimeStatus::Ready),
                ],
            )
            .await;

        assert_eq!(
            changes
                .iter()
                .map(|change| change.revision)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(snapshot.revision, 3);
    }

    #[tokio::test]
    async fn configuration_generation_fences_stale_probe_results() {
        let supervisor = ProviderReadinessSupervisor::default();
        let original = supervisor.cli_generation("ws_default").await;
        let (invalidated, _) = supervisor.invalidate_snapshot("ws_default").await;

        assert_eq!(original, 0);
        assert_eq!(invalidated, 1);
        assert_eq!(supervisor.cli_generation("ws_default").await, invalidated);
        assert!(
            supervisor
                .replace_snapshot_if_generation(
                    "ws_default",
                    original,
                    vec![runtime_summary("codex", RuntimeStatus::Ready)],
                )
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn probe_artifacts_are_published_atomically_and_cleared_by_invalidation() {
        let supervisor = ProviderReadinessSupervisor::default();
        let generation = supervisor.cli_generation("ws_default").await;
        let ready = runtime_summary("codex", RuntimeStatus::Ready);
        let published = supervisor
            .replace_probe_results_if_generation(
                "ws_default",
                generation,
                vec![ready],
                HashMap::from([("codex".to_owned(), model_snapshot("gpt-test"))]),
                HashMap::from([("codex".to_owned(), mcp_readiness())]),
            )
            .await;

        assert!(published.is_some());
        let probe = supervisor
            .runtime_probe_snapshot("ws_default", "codex")
            .await
            .expect("published probe snapshot");
        assert_eq!(
            probe.models.expect("published model cache").result.models[0].id,
            "gpt-test"
        );
        assert!(probe.mcp_readiness.is_some());

        let (next_generation, _) = supervisor.invalidate_snapshot("ws_default").await;
        assert_eq!(next_generation, generation + 1);
        let invalidated = supervisor
            .runtime_probe_snapshot("ws_default", "codex")
            .await
            .expect("invalidated runtime remains visible");
        assert!(invalidated.models.is_none());
        assert!(invalidated.mcp_readiness.is_none());
        assert!(
            supervisor
                .replace_probe_results_if_generation(
                    "ws_default",
                    generation,
                    vec![runtime_summary("codex", RuntimeStatus::Ready)],
                    HashMap::from([("codex".to_owned(), model_snapshot("stale"))]),
                    HashMap::from([("codex".to_owned(), mcp_readiness())]),
                )
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn first_in_flight_probe_is_registered_for_global_invalidation() {
        let supervisor = ProviderReadinessSupervisor::default();

        assert_eq!(supervisor.cli_generation("ws_new").await, 0);
        assert!(supervisor.cli_workspace_ids().await.contains("ws_new"));

        supervisor.invalidate_snapshot("ws_new").await;
        assert_eq!(supervisor.cli_generation("ws_new").await, 1);
    }

    #[tokio::test]
    async fn inactive_workspace_rejects_a_late_in_flight_probe_publication() {
        let supervisor = ProviderReadinessSupervisor::default();
        let generation = supervisor.cli_generation("ws_retired").await;
        supervisor
            .replace_probe_results_if_generation(
                "ws_retired",
                generation,
                vec![runtime_summary("codex", RuntimeStatus::Initializing)],
                HashMap::new(),
                HashMap::new(),
            )
            .await
            .expect("initial workspace snapshot");

        supervisor.retain_workspaces(&HashSet::new()).await;

        assert!(supervisor.snapshot("ws_retired").await.is_none());
        assert!(
            supervisor
                .replace_probe_results_if_generation(
                    "ws_retired",
                    generation,
                    vec![runtime_summary("codex", RuntimeStatus::Ready)],
                    HashMap::from([("codex".to_owned(), model_snapshot("stale"))]),
                    HashMap::from([("codex".to_owned(), mcp_readiness())]),
                )
                .await
                .is_none()
        );
        assert!(supervisor.snapshot("ws_retired").await.is_none());
    }

    #[tokio::test]
    async fn invalidation_atomically_removes_live_readiness_evidence() {
        let supervisor = ProviderReadinessSupervisor::default();
        let generation = supervisor.cli_generation("ws_default").await;
        let mut ready = runtime_summary("codex", RuntimeStatus::Ready);
        ready.capabilities.supports_mcp_tools = true;
        ready.version = Some("1.2.3".to_owned());
        ready.recent_stderr = vec!["old process output".to_owned()];
        supervisor
            .insert_snapshot_if_absent_if_generation("ws_default", generation, vec![ready])
            .await
            .expect("initial seed");

        let (generation, changes) = supervisor.invalidate_snapshot("ws_default").await;
        let snapshot = supervisor.snapshot("ws_default").await.expect("snapshot");

        assert_eq!(generation, 1);
        assert_eq!(snapshot.revision, 2);
        assert_eq!(changes.len(), 1);
        assert!(matches!(
            snapshot.runtimes[0].status,
            RuntimeStatus::Initializing
        ));
        assert!(!snapshot.runtimes[0].capabilities.supports_mcp_tools);
        assert!(snapshot.runtimes[0].version.is_none());
        assert!(snapshot.runtimes[0].recent_stderr.is_empty());
    }

    #[tokio::test]
    async fn snapshot_readers_wait_for_the_fail_closed_invalidation() {
        let supervisor = Arc::new(ProviderReadinessSupervisor::default());
        let generation = supervisor.cli_generation("ws_default").await;
        supervisor
            .insert_snapshot_if_absent_if_generation(
                "ws_default",
                generation,
                vec![runtime_summary("codex", RuntimeStatus::Ready)],
            )
            .await
            .expect("initial seed");

        // Keep invalidation between its generation fence and snapshot write.
        let snapshot_blocker = supervisor.cli_snapshots.read().await;
        let invalidator = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move { supervisor.invalidate_snapshot("ws_default").await })
        };
        for _ in 0..100 {
            if supervisor.cli_generations.try_read().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(supervisor.cli_generations.try_read().is_err());

        let mut reader = {
            let supervisor = supervisor.clone();
            tokio::spawn(async move { supervisor.snapshot("ws_default").await })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut reader)
                .await
                .is_err()
        );

        drop(snapshot_blocker);
        invalidator.await.expect("invalidation task");
        let snapshot = reader.await.expect("snapshot task").expect("snapshot");
        assert!(matches!(
            snapshot.runtimes[0].status,
            RuntimeStatus::Initializing
        ));
    }

    #[tokio::test]
    async fn configuration_change_rejects_a_stale_first_catalog_seed() {
        let supervisor = ProviderReadinessSupervisor::default();
        assert_eq!(supervisor.cli_generation("ws_default").await, 0);
        supervisor.invalidate_snapshot("ws_default").await;

        assert!(
            supervisor
                .insert_snapshot_if_absent_if_generation(
                    "ws_default",
                    0,
                    vec![runtime_summary("codex", RuntimeStatus::Ready)],
                )
                .await
                .is_none()
        );
        assert!(supervisor.snapshot("ws_default").await.is_none());
    }

    #[tokio::test]
    async fn concurrent_snapshot_seeds_share_one_workspace_lock() {
        let supervisor = ProviderReadinessSupervisor::default();

        let first = supervisor.cli_seed_lock("ws_default").await;
        let second = supervisor.cli_seed_lock("ws_default").await;
        let other = supervisor.cli_seed_lock("ws_other").await;

        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[test]
    fn readiness_error_preserves_disabled_runtime_state() {
        let disabled = RuntimeSummary {
            enabled: false,
            ..runtime_summary("claude", RuntimeStatus::Disabled)
        };

        assert_eq!(
            cli_runtime_readiness_error_summary(disabled).status,
            RuntimeStatus::Disabled
        );
    }

    #[test]
    fn concurrent_internal_requests_share_one_gateway_probe() {
        let mut in_flight = HashMap::new();
        let mut rerun = HashMap::new();

        assert!(register_workspace_probe(
            &mut in_flight,
            &mut rerun,
            "ws_default",
            ProviderWarmupScope::CliOnly,
            false,
        ));
        for _ in 0..100 {
            assert!(!register_workspace_probe(
                &mut in_flight,
                &mut rerun,
                "ws_default",
                ProviderWarmupScope::CliOnly,
                false,
            ));
        }
        assert!(rerun.is_empty());
    }

    #[test]
    fn configuration_change_coalesces_to_one_broader_rerun() {
        let mut in_flight = HashMap::from([("ws_default".to_owned(), ProviderWarmupScope::All)]);
        let mut rerun = HashMap::new();

        assert!(!register_workspace_probe(
            &mut in_flight,
            &mut rerun,
            "ws_default",
            ProviderWarmupScope::CliOnly,
            true,
        ));
        assert!(!register_workspace_probe(
            &mut in_flight,
            &mut rerun,
            "ws_default",
            ProviderWarmupScope::ApiOnly,
            true,
        ));
        assert_eq!(rerun.len(), 1);
        assert_eq!(rerun["ws_default"], ProviderWarmupScope::All);
    }

    #[test]
    fn uncovered_scope_is_not_lost_behind_an_existing_warmup() {
        let mut in_flight =
            HashMap::from([("ws_default".to_owned(), ProviderWarmupScope::ApiOnly)]);
        let mut rerun = HashMap::new();

        assert!(!register_workspace_probe(
            &mut in_flight,
            &mut rerun,
            "ws_default",
            ProviderWarmupScope::All,
            false,
        ));
        assert_eq!(rerun["ws_default"], ProviderWarmupScope::All);
    }

    #[test]
    fn retry_backoff_is_bounded() {
        assert_eq!(provider_retry_delay(0), Duration::from_secs(30));
        assert_eq!(provider_retry_delay(1), Duration::from_secs(60));
        assert_eq!(provider_retry_delay(2), Duration::from_secs(120));
        assert_eq!(provider_retry_delay(20), Duration::from_secs(5 * 60));
    }

    #[test]
    fn successful_partial_probe_preserves_only_unchecked_retry_scope() {
        let mut retries = HashMap::from([("ws_default".to_owned(), ProviderWarmupScope::All)]);

        complete_retry_scope(
            &mut retries,
            "ws_default",
            ProviderWarmupScope::ApiOnly,
            None,
        );

        assert_eq!(retries["ws_default"], ProviderWarmupScope::CliOnly);
    }

    #[test]
    fn failed_partial_probe_merges_with_existing_retry_scope() {
        let mut retries = HashMap::from([("ws_default".to_owned(), ProviderWarmupScope::ApiOnly)]);

        complete_retry_scope(
            &mut retries,
            "ws_default",
            ProviderWarmupScope::CliOnly,
            Some(ProviderWarmupScope::CliOnly),
        );

        assert_eq!(retries["ws_default"], ProviderWarmupScope::All);
    }

    #[test]
    fn successful_full_probe_clears_retry_state() {
        let mut retries = HashMap::from([("ws_default".to_owned(), ProviderWarmupScope::All)]);

        complete_retry_scope(&mut retries, "ws_default", ProviderWarmupScope::All, None);

        assert!(retries.is_empty());
    }
}
