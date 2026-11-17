//! Workspace runtime snapshots and bounded, demand-owned reconciliation.

use crate::core::*;
use pioneer_protocol::{
    CLIRuntimeListParams, CLIRuntimeListResponse, GatewayNotification, RuntimeSummary,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak, mpsc},
    time::{Duration, Instant},
};

const RETRY_DELAYS: [Duration; 4] = [
    Duration::ZERO,
    Duration::from_millis(500),
    Duration::from_secs(2),
    Duration::from_secs(5),
];

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderRuntimeIntent {
    Observe { workspace_id: String },
    Release { workspace_id: String },
    Refresh { workspace_id: String },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderRuntimeRequestState {
    #[default]
    Idle,
    Loading,
    Ready,
    Failed,
    Cancelled,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct ProviderRuntimePublication {
    workspace_id: String,
    revision: u64,
    runtimes: Vec<Arc<ProviderRuntimeRow>>,
    request: ProviderRuntimeRequestState,
}
impl ProviderRuntimePublication {
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn runtimes(&self) -> &[Arc<ProviderRuntimeRow>] {
        &self.runtimes
    }
    pub fn request(&self) -> &ProviderRuntimeRequestState {
        &self.request
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct ProviderRuntimeRow {
    id: String,
    revision: u64,
    #[serde(flatten)]
    runtime: RuntimeSummary,
}
impl ProviderRuntimeRow {
    pub fn runtime(&self) -> &RuntimeSummary {
        &self.runtime
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    fn replace(previous: Option<&Arc<Self>>, mut runtime: RuntimeSummary) -> Arc<Self> {
        runtime.proxy_url = super::credentials::public_proxy(runtime.proxy_url.take());
        runtime.recent_stderr =
            pioneer_protocol::sanitize_runtime_diagnostic_lines(runtime.recent_stderr);
        for diagnostic in &mut runtime.diagnostics {
            diagnostic.code = pioneer_protocol::sanitize_runtime_diagnostic_line(&diagnostic.code);
            diagnostic.message =
                pioneer_protocol::sanitize_runtime_diagnostic_line(&diagnostic.message);
        }
        match &mut runtime.status {
            pioneer_protocol::RuntimeStatus::Error { message }
            | pioneer_protocol::RuntimeStatus::SpawnFailed { message }
            | pioneer_protocol::RuntimeStatus::Degraded { message } => {
                *message = pioneer_protocol::sanitize_runtime_diagnostic_line(message)
            }
            _ => {}
        }

        if let Some(previous) = previous.filter(|previous| previous.runtime == runtime) {
            return previous.clone();
        }
        Arc::new(Self {
            id: runtime.runtime_id.clone(),
            revision: previous.map_or(1, |p| {
                p.revision
                    .checked_add(1)
                    .expect("runtime row revision exhausted")
            }),
            runtime,
        })
    }
}
impl std::ops::Deref for ProviderRuntimeRow {
    type Target = RuntimeSummary;
    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuntimeRequest {
    workspace_id: String,
    pub(crate) generation: u64,
    auth: (u64, u64, Option<u64>),
    pub(crate) attempt: usize,
    refresh_runtime: Option<Option<String>>,
}
/// A retained consumer's demand. Dropping it wakes cancellation without calling
/// back into a consumer while its store lock is held.
pub(crate) struct ProviderRuntimeDemand {
    queue: Option<pioneer_observability::queues::QueueObserver>,
    token: Option<Arc<()>>,
    wake: Option<mpsc::SyncSender<()>>,
}
impl Drop for ProviderRuntimeDemand {
    fn drop(&mut self) {
        self.token.take();
        if let Some(wake) = &self.wake {
            send_runtime_wake(self.queue.as_ref(), wake);
        }
    }
}
// Both producers wake the same reconciliation lane; a full slot is merged input.
fn send_runtime_wake(
    queue: Option<&pioneer_observability::queues::QueueObserver>,
    wake: &mpsc::SyncSender<()>,
) {
    use pioneer_observability::queues::{QueueBytes, QueueObserver};
    let queue = queue.map(QueueObserver::in_current_epoch);
    if let Some(queue) = &queue {
        queue.enqueued(QueueBytes::Known(0));
    }
    match wake.try_send(()) {
        Ok(()) => {}
        Err(mpsc::TrySendError::Full(_)) => {
            if let Some(queue) = &queue {
                queue.coalesced_send(QueueBytes::Known(0));
            }
        }
        Err(mpsc::TrySendError::Disconnected(_)) => {
            if let Some(queue) = &queue {
                queue.rejected_send(QueueBytes::Known(0));
            }
        }
    }
}
struct RuntimeScope {
    publication: Arc<ProviderRuntimePublication>,
    auth: (u64, u64, Option<u64>),
    source_revision: u64,
    minimum_revision: u64,
    loaded: bool,
    // Accepted data belongs to authorization, while requests also belong to a connection.
    accepted_auth: Option<(u64, u64)>,
    prefetch: bool,
    demand: usize,
    retained_demand: Vec<Weak<()>>,
    listeners: Vec<mpsc::SyncSender<()>>,
    request: Option<RuntimeRequest>,
    due: Option<Instant>,
}
impl RuntimeScope {
    fn demanded(&self) -> bool {
        self.demand > 0
            || self
                .retained_demand
                .iter()
                .any(|demand| demand.strong_count() > 0)
    }
}
#[derive(Default)]
pub(crate) struct ProviderRuntimeController {
    queue: Option<pioneer_observability::queues::QueueObserver>,
    scopes: BTreeMap<String, RuntimeScope>,
    generation: u64,
    wake: Option<mpsc::SyncSender<()>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl ProviderRuntimeController {
    pub(crate) fn stop(&mut self) {
        self.wake.take();
        self.scopes.clear();
    }
    fn wake(&self) {
        if let Some(wake) = &self.wake {
            send_runtime_wake(self.queue.as_ref(), wake);
        }
    }
    fn schedule(&mut self, workspace: &str, auth: (u64, u64, Option<u64>)) {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("provider runtime generation exhausted");
        let scope = self
            .scopes
            .get_mut(workspace)
            .expect("registered runtime scope");
        scope.request = Some(RuntimeRequest {
            workspace_id: workspace.into(),
            generation: self.generation,
            auth,
            attempt: 0,
            refresh_runtime: None,
        });
        scope.due = Some(Instant::now());
        self.wake();
    }
}
impl Drop for ProviderRuntimeController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
impl ClientCore {
    pub(crate) fn provider_runtime_epoch(&self) -> (u64, u64, Option<u64>) {
        self.snapshot(&ClientScope::Administration { workspace_id: None })
            .and_then(|p| p.snapshot().payload::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>())
            .map_or((0, 0, None), |p| (p.connection_generation, p.authorization_change_sequence, p.connection_id))
    }
    /// Blocking adapter for background callers; demand and retries stay in this controller.
    pub fn read_provider_runtimes(
        &self,
        workspace: &str,
        refresh: bool,
    ) -> anyhow::Result<CLIRuntimeListResponse> {
        self.read_provider_runtime_request(workspace, refresh, None)
    }
    /// The Send consumer shares the normal runtime owner, with an individual
    /// deadline/cancel lifetime for its wait.
    pub(crate) fn read_provider_runtimes_with_budget(
        &self,
        workspace: &str,
        refresh: bool,
        budget: &crate::rpc::RequestBudget,
    ) -> anyhow::Result<CLIRuntimeListResponse> {
        budget.notify_on_cancel(&self.send_readiness);
        budget.check()?;
        self.read_provider_runtime_request_with_wait(workspace, refresh, None, Some(budget), |_| {
            unreachable!("budgeted consumer uses readiness signal")
        })
    }
    /// Preserves the explicit Gateway probe operation for existing typed callers.
    pub fn refresh_provider_runtimes(
        &self,
        params: pioneer_protocol::CLIRuntimeRefreshParams,
    ) -> anyhow::Result<pioneer_protocol::CLIRuntimeRefreshResponse> {
        let response = self.read_provider_runtime_request(
            &params.workspace_id,
            false,
            Some(params.runtime_id),
        )?;
        Ok(pioneer_protocol::CLIRuntimeRefreshResponse {
            revision: response.revision,
            runtimes: response.runtimes,
        })
    }
    fn schedule_provider_runtime_refresh(
        &self,
        workspace: &str,
        runtime_id: Option<String>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.provider_runtime_allowed(workspace),
            "provider_runtime_unavailable"
        );
        let epoch = self.provider_runtime_epoch();
        let mut owner = self
            .provider_runtimes
            .lock()
            .expect("provider runtimes poisoned");
        let scope = owner
            .scopes
            .get(workspace)
            .ok_or_else(|| anyhow::anyhow!("provider_runtime_unavailable"))?;
        if scope.request.as_ref().is_some_and(|request| {
            request.auth == epoch && request.refresh_runtime.as_ref() == Some(&runtime_id)
        }) {
            return Ok(());
        }
        owner.schedule(workspace, epoch);
        let scope = owner.scopes.get_mut(workspace).unwrap();
        scope.request.as_mut().unwrap().refresh_runtime = Some(runtime_id);
        let mut next = (*scope.publication).clone();
        next.request = ProviderRuntimeRequestState::Loading;
        self.publish_provider_runtime(scope, next);
        Ok(())
    }
    fn read_provider_runtime_request(
        &self,
        workspace: &str,
        refresh: bool,
        probe: Option<Option<String>>,
    ) -> anyhow::Result<CLIRuntimeListResponse> {
        self.read_provider_runtime_request_with_wait(workspace, refresh, probe, None, |receiver| {
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Timeout) => Ok(()),
                Err(_) => anyhow::bail!("provider_runtime_cancelled"),
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn read_provider_runtimes_with_events_for_test(
        &self,
        workspace: &str,
        mut event: impl FnMut(),
    ) -> anyhow::Result<CLIRuntimeListResponse> {
        self.read_provider_runtime_request_with_wait(workspace, true, None, None, |_| {
            event();
            Ok(())
        })
    }

    fn read_provider_runtime_request_with_wait(
        &self,
        workspace: &str,
        refresh: bool,
        probe: Option<Option<String>>,
        budget: Option<&crate::rpc::RequestBudget>,
        mut wait: impl FnMut(&mpsc::Receiver<()>) -> anyhow::Result<()>,
    ) -> anyhow::Result<CLIRuntimeListResponse> {
        anyhow::ensure!(
            !workspace.trim().is_empty() && !self.is_stopped(),
            "provider_runtime_unavailable"
        );
        let mut epoch = self.provider_runtime_epoch();
        let mut connection_transitions = 0;
        let allow_connection_transition = probe.is_none();
        let _demand = self.retain_provider_runtime(workspace);
        if let Some(runtime_id) = probe {
            self.schedule_provider_runtime_refresh(workspace, runtime_id)?;
        } else if refresh {
            self.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
                workspace_id: workspace.into(),
            });
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        {
            let mut owner = self
                .provider_runtimes
                .lock()
                .expect("provider runtimes poisoned");
            let scope = owner
                .scopes
                .get_mut(workspace)
                .ok_or_else(|| anyhow::anyhow!("provider_runtime_unavailable"))?;
            scope.listeners.retain(|listener| {
                !matches!(
                    listener.try_send(()),
                    Err(mpsc::TrySendError::Disconnected(_))
                )
            });
            anyhow::ensure!(scope.listeners.len() < 64, "provider_runtime_overloaded");
            scope.listeners.push(sender);
        }
        loop {
            let sequence = self.send_readiness_sequence();
            if let Some(budget) = budget {
                budget.check()?;
            }
            let current = self.provider_runtime_epoch();
            anyhow::ensure!(
                !self.is_stopped()
                    && (current.0, current.1) == (epoch.0, epoch.1)
                    && self.provider_runtime_allowed(workspace),
                "provider_runtime_cancelled"
            );
            if current != epoch {
                // Only the safe list read can cross at most two connection replacements.
                anyhow::ensure!(
                    allow_connection_transition && connection_transitions < 2,
                    "provider_runtime_cancelled"
                );
                connection_transitions += 1;
                epoch = current;
                let current_request = self
                    .provider_runtimes
                    .lock()
                    .expect("provider runtimes poisoned")
                    .scopes
                    .get(workspace)
                    .is_some_and(|scope| {
                        scope.auth == epoch && (scope.loaded || scope.request.is_some())
                    });
                if !current_request {
                    self.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
                        workspace_id: workspace.into(),
                    });
                }
            }
            {
                let owner = self
                    .provider_runtimes
                    .lock()
                    .expect("provider runtimes poisoned");
                let scope = owner
                    .scopes
                    .get(workspace)
                    .ok_or_else(|| anyhow::anyhow!("provider_runtime_cancelled"))?;
                anyhow::ensure!(
                    scope.auth == epoch && scope.demanded(),
                    "provider_runtime_cancelled"
                );
                match scope.publication.request {
                    ProviderRuntimeRequestState::Ready => {
                        return Ok(CLIRuntimeListResponse {
                            revision: scope.source_revision,
                            runtimes: scope
                                .publication
                                .runtimes
                                .iter()
                                .map(|row| row.runtime.clone())
                                .collect(),
                        });
                    }
                    ProviderRuntimeRequestState::Loading => {}
                    _ => anyhow::bail!("provider_runtime_unavailable"),
                }
            }
            if let Some(budget) = budget {
                self.wait_send_readiness(sequence, budget.deadline())?;
            } else {
                wait(&receiver)?;
            }
        }
    }
    pub(crate) fn resume_provider_runtime_demand(&self) {
        let workspaces: Vec<_> = self
            .provider_runtimes
            .lock()
            .expect("provider runtimes poisoned")
            .scopes
            .iter()
            .filter(|(_, scope)| {
                scope.demanded()
                    && scope.publication.request == ProviderRuntimeRequestState::Cancelled
            })
            .map(|(id, _)| id.clone())
            .collect();
        for workspace_id in workspaces {
            self.provider_runtime_intent(ProviderRuntimeIntent::Refresh { workspace_id });
        }
    }
    fn provider_runtime_allowed(&self, workspace: &str) -> bool {
        self.authorization_snapshot(Some(workspace), None)
            .or_else(|| self.authorization_snapshot(None, None))
            .is_some_and(|p| {
                let capabilities = crate::authorization::principal_presentation_capabilities(&p);
                capabilities.can_manage_capabilities || capabilities.can_use_cli_runtimes
            })
    }
    pub(crate) fn provider_runtime_data_valid(&self, workspace: &str) -> bool {
        let auth = self.provider_runtime_epoch();
        self.provider_runtimes
            .lock()
            .expect("provider runtimes poisoned")
            .scopes
            .get(workspace)
            .is_some_and(|scope| scope.accepted_auth == Some((auth.0, auth.1)))
    }
    pub fn provider_runtime_snapshot(
        &self,
        workspace_id: &str,
    ) -> Option<Arc<ProviderRuntimePublication>> {
        self.snapshot(&ClientScope::ProviderRuntime {
            workspace_id: workspace_id.into(),
        })
        .and_then(|p| p.snapshot().payload())
    }
    fn publish_provider_runtime(
        &self,
        scope: &mut RuntimeScope,
        mut next: ProviderRuntimePublication,
    ) -> ClientTransition {
        next.revision = scope.publication.revision;
        if next == *scope.publication {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        next.revision = next
            .revision
            .max(
                self.snapshot(&ClientScope::ProviderRuntime {
                    workspace_id: next.workspace_id.clone(),
                })
                .map_or(0, |p| p.revisions().scoped().get()),
            )
            .checked_add(1)
            .expect("provider runtime revision exhausted");
        // Runtime -> collection is the lock order. Collection completion never
        // acquires the runtime owner. Fence old catalog RPCs before publishing
        // evidence that can wake model consumers on another thread.
        let changed = scope
            .publication
            .runtimes
            .iter()
            .map(|row| row.runtime_id.clone())
            .chain(next.runtimes.iter().map(|row| row.runtime_id.clone()))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .filter(|id| {
                super::list::cli_runtime_catalog_changed(
                    scope
                        .publication
                        .runtimes
                        .iter()
                        .find(|row| &row.runtime_id == id)
                        .map(|row| row.runtime()),
                    next.runtimes
                        .iter()
                        .find(|row| &row.runtime_id == id)
                        .map(|row| row.runtime()),
                )
            })
            .collect::<Vec<_>>();
        self.invalidate_cli_runtime_model_collections(&next.workspace_id, &changed);
        let revision = next.revision;
        let workspace_id = next.workspace_id.clone();
        scope.publication = Arc::new(next);
        self.notify_send_readiness();
        scope.listeners.retain(|listener| {
            !matches!(
                listener.try_send(()),
                Err(mpsc::TrySendError::Disconnected(_))
            )
        });
        self.publish(
            &ClientMutationAuthority { _private: () },
            ClientScope::ProviderRuntime { workspace_id },
            crate::threads::registry::revisions(revision),
            scope.publication.clone(),
            vec![],
        )
    }
    /// Guard buffered consumer responses too, including the interval between
    /// runtime publication and reconciliation of that consumer's generation.
    pub(crate) fn cli_catalog_matches_runtime_publication(
        &self,
        workspace: &str,
        provider: Option<&str>,
        known: &[RuntimeSummary],
    ) -> bool {
        let Some(id) = provider.and_then(super::list::runtime_id_from_cli_runtime_provider_key)
        else {
            return true;
        };
        let snapshot = self.provider_runtime_snapshot(workspace);
        !super::list::cli_runtime_catalog_changed(
            known.iter().find(|runtime| runtime.runtime_id == id),
            snapshot.as_ref().and_then(|p| {
                p.runtimes()
                    .iter()
                    .find(|row| row.runtime().runtime_id == id)
                    .map(|row| row.runtime())
            }),
        )
    }

    pub fn provider_runtime_intent(&self, intent: ProviderRuntimeIntent) -> ClientTransition {
        let workspace = match &intent {
            ProviderRuntimeIntent::Observe { workspace_id }
            | ProviderRuntimeIntent::Release { workspace_id }
            | ProviderRuntimeIntent::Refresh { workspace_id } => workspace_id.clone(),
        };
        let transition = self.reduce_provider_runtime_intent(intent, None);
        self.sync_composer_provider_runtimes(workspace.trim());
        transition
    }
    pub(crate) fn retain_provider_runtime(&self, workspace: &str) -> ProviderRuntimeDemand {
        let token = Arc::new(());
        self.reduce_provider_runtime_intent(
            ProviderRuntimeIntent::Observe {
                workspace_id: workspace.into(),
            },
            Some(Arc::downgrade(&token)),
        );
        let owner = self
            .provider_runtimes
            .lock()
            .expect("provider runtimes poisoned");
        let wake = owner.wake.clone();
        let queue = owner.queue.clone();
        ProviderRuntimeDemand {
            queue,
            token: Some(token),
            wake,
        }
    }
    fn reduce_provider_runtime_intent(
        &self,
        intent: ProviderRuntimeIntent,
        retained: Option<Weak<()>>,
    ) -> ClientTransition {
        let (workspace, observe, release) = match intent {
            ProviderRuntimeIntent::Observe { workspace_id } => (workspace_id, true, false),
            ProviderRuntimeIntent::Release { workspace_id } => (workspace_id, false, true),
            ProviderRuntimeIntent::Refresh { workspace_id } => (workspace_id, false, false),
        };
        let workspace = workspace.trim();
        if workspace.is_empty() || self.is_stopped() {
            return self.reject_intent();
        }
        let auth = self.provider_runtime_epoch();
        let allowed = self.provider_runtime_allowed(workspace);
        let mut owner = self
            .provider_runtimes
            .lock()
            .expect("provider runtimes poisoned");
        if release && !owner.scopes.contains_key(workspace) {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        let scope = owner
            .scopes
            .entry(workspace.into())
            .or_insert_with(|| RuntimeScope {
                publication: Arc::new(ProviderRuntimePublication {
                    workspace_id: workspace.into(),
                    revision: self
                        .snapshot(&ClientScope::ProviderRuntime {
                            workspace_id: workspace.into(),
                        })
                        .map_or(0, |p| p.revisions().scoped().get()),
                    runtimes: vec![],
                    request: ProviderRuntimeRequestState::Idle,
                }),
                auth,
                source_revision: 0,
                minimum_revision: 0,
                loaded: false,
                accepted_auth: None,
                prefetch: false,
                demand: 0,
                retained_demand: Vec::new(),
                listeners: Vec::new(),
                request: None,
                due: None,
            });
        if release {
            scope.demand = scope.demand.saturating_sub(1);
            if scope.demanded() || scope.prefetch || scope.request.is_none() {
                return self.navigation_outcome(ClientTransitionOutcome::Noop);
            }
            scope.request = None;
            scope.due = None;
            let mut next = (*scope.publication).clone();
            next.request = ProviderRuntimeRequestState::Cancelled;
            return self.publish_provider_runtime(scope, next);
        }
        // A newly retained consumer must not adopt work whose last owner was
        // dropped, even if the worker has not processed its cancellation wake yet.
        if !scope.demanded() && !scope.prefetch {
            scope.request = None;
            scope.due = None;
        }
        if !observe && !scope.demanded() {
            scope.prefetch = true;
        }
        if observe {
            scope.prefetch = false;
        }
        scope
            .retained_demand
            .retain(|demand| demand.strong_count() > 0);
        if let Some(retained) = retained {
            scope.retained_demand.push(retained);
        } else if observe {
            scope.demand = scope
                .demand
                .checked_add(1)
                .expect("provider runtime demand exhausted");
        }
        let epoch_changed = scope.auth != auth;
        if epoch_changed {
            scope.auth = auth;
            scope.loaded = false;
            if scope.accepted_auth != Some((auth.0, auth.1)) {
                scope.accepted_auth = None;
            }
            // Revision fences belong to this connection's stream. Accepted rows
            // remain useful while its replacement obtains a new full snapshot.
            scope.source_revision = 0;
            scope.minimum_revision = 0;
            scope.request = None;
            scope.due = None;
        }
        if !allowed {
            scope.request = None;
            scope.due = None;
            scope.prefetch = false;
            scope.loaded = false;
            scope.accepted_auth = None;
            let mut next = (*scope.publication).clone();
            next.runtimes.clear();
            next.request = ProviderRuntimeRequestState::Cancelled;
            return self.publish_provider_runtime(scope, next);
        }
        if scope.request.is_some() {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        if observe
            && !epoch_changed
            && scope.publication.request == ProviderRuntimeRequestState::Failed
        {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        if observe && scope.loaded {
            if matches!(
                scope.publication.request,
                ProviderRuntimeRequestState::Loading | ProviderRuntimeRequestState::Cancelled
            ) {
                let mut next = (*scope.publication).clone();
                next.request = ProviderRuntimeRequestState::Ready;
                return self.publish_provider_runtime(scope, next);
            }
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        owner.schedule(workspace, auth);
        let scope = owner
            .scopes
            .get_mut(workspace)
            .expect("registered runtime scope");
        let mut next = (*scope.publication).clone();
        if scope.accepted_auth != Some((auth.0, auth.1)) {
            next.runtimes.clear();
        }
        next.request = ProviderRuntimeRequestState::Loading;
        self.publish_provider_runtime(scope, next)
    }
    fn complete_provider_runtime(
        &self,
        request: &RuntimeRequest,
        result: Result<CLIRuntimeListResponse, ()>,
        now: Instant,
    ) -> ClientTransition {
        let transition = self.reduce_provider_runtime_completion(request, result, now);
        self.sync_composer_provider_runtimes(&request.workspace_id);
        transition
    }
    fn reduce_provider_runtime_completion(
        &self,
        request: &RuntimeRequest,
        result: Result<CLIRuntimeListResponse, ()>,
        now: Instant,
    ) -> ClientTransition {
        let auth = self.provider_runtime_epoch();
        if !self.provider_runtime_allowed(&request.workspace_id) {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        let mut owner = self
            .provider_runtimes
            .lock()
            .expect("provider runtimes poisoned");
        let Some(scope) = owner.scopes.get_mut(&request.workspace_id) else {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        };
        if self.is_stopped()
            || auth != request.auth
            || scope.request.as_ref() != Some(request)
            || (!scope.demanded() && !scope.prefetch)
        {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        let mut next = (*scope.publication).clone();
        match result {
            Ok(response)
                if response.revision >= scope.minimum_revision
                    && response.revision >= scope.source_revision
                    && {
                        let mut ids = std::collections::BTreeSet::new();
                        response.runtimes.iter().all(|runtime| {
                            !runtime.runtime_id.trim().is_empty()
                                && ids.insert(runtime.runtime_id.as_str())
                        })
                    } =>
            {
                next.runtimes = response
                    .runtimes
                    .into_iter()
                    .map(|runtime| {
                        ProviderRuntimeRow::replace(
                            scope
                                .publication
                                .runtimes
                                .iter()
                                .find(|previous| previous.runtime_id == runtime.runtime_id),
                            runtime,
                        )
                    })
                    .collect();
                scope.source_revision = response.revision;
                scope.minimum_revision = scope.minimum_revision.max(response.revision);
                scope.loaded = true;
                scope.accepted_auth = Some((auth.0, auth.1));
                scope.prefetch = false;
                scope.request = None;
                scope.due = None;
                next.request = ProviderRuntimeRequestState::Ready;
            }
            _ if (scope.demanded() || scope.prefetch)
                && request.attempt + 1 < RETRY_DELAYS.len() =>
            {
                let mut retry = request.clone();
                retry.attempt += 1;
                scope.due = Some(now + RETRY_DELAYS[retry.attempt]);
                scope.request = Some(retry);
            }
            _ => {
                scope.request = None;
                scope.due = None;
                scope.prefetch = false;
                next.request = ProviderRuntimeRequestState::Failed;
            }
        }
        self.publish_provider_runtime(scope, next)
    }
    pub(crate) fn invalidate_provider_runtimes(&self) {
        let mut owner = self
            .provider_runtimes
            .lock()
            .expect("provider runtimes poisoned");
        for scope in owner.scopes.values_mut() {
            scope.request = None;
            scope.due = None;
            scope.prefetch = false;
            scope.loaded = false;
            scope.accepted_auth = None;
            scope.source_revision = 0;
            scope.minimum_revision = 0;
            let mut next = (*scope.publication).clone();
            next.runtimes.clear();
            next.request = ProviderRuntimeRequestState::Cancelled;
            scope.publication = Arc::new(next);
        }
        self.notify_send_readiness();
        owner.wake();
    }
    pub(crate) fn observe_provider_runtime_notification(&self, notification: &GatewayNotification) {
        self.reduce_provider_runtime_notification(notification);
        let workspace = match notification {
            GatewayNotification::CLIRuntimeStatusChanged(update) => &update.workspace_id,
            GatewayNotification::CLIRuntimeAccountUpdated(update) => &update.workspace_id,
            GatewayNotification::CLIRuntimeAppsChanged(update) => &update.workspace_id,
            _ => return,
        };
        self.sync_composer_provider_runtimes(workspace);
    }
    /// Retained rows are display evidence only until a read admitted after the
    /// invalidation completes. A queued read can absorb more invalidations; an
    /// already dispatched read must lose its identity before it can complete.
    fn invalidate_provider_runtime_scope(
        &self,
        owner: &mut ProviderRuntimeController,
        workspace: &str,
    ) {
        let scope = owner.scopes.get_mut(workspace).unwrap();
        scope.loaded = false;
        let needs_read = scope.demanded() || scope.prefetch;
        let schedule = needs_read && (scope.request.is_none() || scope.due.is_none());
        let auth = scope.auth;
        let attempt = scope.request.as_ref().map_or(0, |request| request.attempt);
        if !needs_read {
            scope.request = None;
            scope.due = None;
        }
        if schedule {
            owner.schedule(workspace, auth);
            // Invalidation preserves the active retry budget. An in-flight
            // explicit probe is followed by a list, never replayed as a probe.
            owner
                .scopes
                .get_mut(workspace)
                .unwrap()
                .request
                .as_mut()
                .unwrap()
                .attempt = attempt;
        }
        let scope = owner.scopes.get_mut(workspace).unwrap();
        let mut next = (*scope.publication).clone();
        next.request = ProviderRuntimeRequestState::Loading;
        self.publish_provider_runtime(scope, next);
    }
    fn reduce_provider_runtime_notification(&self, notification: &GatewayNotification) {
        let workspace = match notification {
            GatewayNotification::CLIRuntimeStatusChanged(p) => &p.workspace_id,
            GatewayNotification::CLIRuntimeAccountUpdated(p) => &p.workspace_id,
            GatewayNotification::CLIRuntimeAppsChanged(p) => &p.workspace_id,
            _ => return,
        };
        if !self.provider_runtime_allowed(workspace) {
            return;
        }
        let auth = self.provider_runtime_epoch();
        let GatewayNotification::CLIRuntimeStatusChanged(update) = notification else {
            let workspace = match notification {
                GatewayNotification::CLIRuntimeAccountUpdated(update) => &update.workspace_id,
                GatewayNotification::CLIRuntimeAppsChanged(update) => &update.workspace_id,
                _ => return,
            };
            let mut owner = self
                .provider_runtimes
                .lock()
                .expect("provider runtimes poisoned");
            if owner
                .scopes
                .get(workspace)
                .is_some_and(|scope| scope.auth == auth)
            {
                self.invalidate_provider_runtime_scope(&mut owner, workspace);
            }
            return;
        };
        let mut owner = self
            .provider_runtimes
            .lock()
            .expect("provider runtimes poisoned");
        let Some(scope) = owner.scopes.get_mut(&update.workspace_id) else {
            return;
        };
        if scope.auth != auth {
            return;
        }
        scope.minimum_revision = scope.minimum_revision.max(update.revision);
        if update.revision != 0 && update.revision <= scope.source_revision {
            return;
        }
        if !scope.loaded
            || update.revision == 0
            || update.revision != scope.source_revision.saturating_add(1)
        {
            self.invalidate_provider_runtime_scope(&mut owner, &update.workspace_id);
            return;
        }
        scope.source_revision = update.revision;
        let mut next = (*scope.publication).clone();
        if update.removed {
            next.runtimes
                .retain(|runtime| runtime.runtime_id != update.runtime.runtime_id);
        } else if let Some(ix) = next
            .runtimes
            .iter()
            .position(|runtime| runtime.runtime_id == update.runtime.runtime_id)
        {
            next.runtimes[ix] =
                ProviderRuntimeRow::replace(Some(&next.runtimes[ix]), update.runtime.clone());
        } else {
            next.runtimes
                .push(ProviderRuntimeRow::replace(None, update.runtime.clone()));
        }
        self.publish_provider_runtime(scope, next);
    }
    pub(crate) fn start_provider_runtime_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel(1);
        let queue = pioneer_observability::queues::QueueObserver::new(
            pioneer_observability::queues::QueueKind::ClientController,
            Some(1),
            "coalescing wake tokens only; zero payload bytes; domain state is reconciled separately",
        );
        let receiver_queue = queue.clone();
        let weak = Arc::downgrade(self);
        let mut owner = self
            .provider_runtimes
            .lock()
            .expect("provider runtimes poisoned");
        owner.queue = Some(queue);
        owner.wake = Some(sender);
        owner.task = Some(
            std::thread::Builder::new()
                .name("client-provider-runtimes".into())
                .spawn(move || {
                    loop {
                    let cycle=pioneer_observability::work::WorkCarrier::independent(pioneer_observability::work::Boundary::Timer,pioneer_observability::work::WorkKind::ClientCommand,pioneer_observability::work::Trigger::Reconcile,pioneer_observability::work::ExecutorRole::ClientController);
                    let _cycle_scope=cycle.enter();
                    let mut cycle_guard = pioneer_observability::live::WorkGuard::start(cycle.context(), false);
                        let Some(core) = weak.upgrade() else {
                            break;
                        };
                        if core.is_stopped() {
                            break;
                        }
                        let (request, delay) = {
                            let mut owner = core
                                .provider_runtimes
                                .lock()
                                .expect("provider runtimes poisoned");
                            let now = Instant::now();
                            for scope in owner.scopes.values_mut() {
                                if !scope.demanded() && !scope.prefetch && scope.request.is_some() {
                                    scope.request = None;
                                    scope.due = None;
                                    let mut next = (*scope.publication).clone();
                                    next.request = ProviderRuntimeRequestState::Cancelled;
                                    core.publish_provider_runtime(scope, next);
                                }
                            }
                            let next = owner
                                .scopes
                                .values_mut()
                                .filter(|s| s.due.is_some())
                                .min_by_key(|s| s.due);
                            match next {
                                Some(scope) if scope.due.is_some_and(|due| due <= now) => {
                                    scope.due = None;
                                    (scope.request.clone(), None)
                                }
                                Some(scope) => (
                                    None,
                                    scope.due.map(|due| due.saturating_duration_since(now)),
                                ),
                                None => (None, None),
                            }
                        };
                        if let Some(request) = request {
                            let matches = core.provider_runtime_epoch() == request.auth
                                && core
                                    .provider_runtimes
                                    .lock()
                                    .expect("provider runtimes poisoned")
                                    .scopes
                                    .get(&request.workspace_id)
                                    .is_some_and(|scope| {
                                        scope.request.as_ref() == Some(&request)
                                            && (scope.demanded() || scope.prefetch)
                                    });
                            if !matches {
                                cycle_guard.finish(pioneer_observability::work::Outcome::Completed);
                        continue;
                            }
                            let sender = core.transport_runtime().ws_command_sender();
                            drop(core);
                            let Some(connection) = request.auth.2 else {
                                if let Some(core) = weak.upgrade() {
                                    core.complete_provider_runtime(
                                        &request,
                                        Err(()),
                                        Instant::now(),
                                    );
                                }
                                cycle_guard.finish(pioneer_observability::work::Outcome::Completed);
                        continue;
                            };
                            let transport = sender.requests_for_connection(connection);
                            let result = if let Some(runtime_id) = &request.refresh_runtime {
                                crate::transport::ws::command_sender::cli_runtime_refresh(
                                    &transport,
                                    pioneer_protocol::CLIRuntimeRefreshParams {
                                        workspace_id: request.workspace_id.clone(),
                                        runtime_id: runtime_id.clone(),
                                    },
                                )
                                .map(|response| {
                                    CLIRuntimeListResponse {
                                        revision: response.revision,
                                        runtimes: response.runtimes,
                                    }
                                })
                            } else {
                                crate::transport::ws::command_sender::cli_runtime_list(
                                    &transport,
                                    CLIRuntimeListParams {
                                        workspace_id: request.workspace_id.clone(),
                                    },
                                )
                            }
                            .map_err(|_| ());
                            let Some(core) = weak.upgrade() else {
                                break;
                            };
                            core.complete_provider_runtime(&request, result, Instant::now());
                        } else {
                            drop(core);
                            cycle_guard.finish(pioneer_observability::work::Outcome::Completed);
                    match delay {
                                Some(delay) => {
                                    if matches!(
                                        receiver.recv_timeout(delay).map(|()| { receiver_queue.dequeued(pioneer_observability::queues::QueueBytes::Known(0)); receiver_queue.outcome(pioneer_observability::queues::QueueOutcome::Processed, 1); }),
                                        Err(mpsc::RecvTimeoutError::Disconnected)
                                    ) {
                                        break;
                                    }
                                }
                                None => {
                                    if receiver.recv().map(|()| { receiver_queue.dequeued(pioneer_observability::queues::QueueBytes::Known(0)); receiver_queue.outcome(pioneer_observability::queues::QueueOutcome::Processed, 1); }).is_err() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                })
                .expect("provider runtime worker could not start"),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retained_demand_drop_coalesces_a_pending_wake_and_pins_epoch() {
        use pioneer_observability::{
            live,
            queues::{QueueBytes, QueueKind, QueueObserver},
        };
        let _lock = crate::OBSERVABILITY_TEST_LOCK.lock().unwrap();
        let old = pioneer_observability::telemetry_enabled();
        pioneer_observability::set_telemetry_enabled(false);
        pioneer_observability::set_telemetry_enabled(true);
        let queue = QueueObserver::new(QueueKind::ClientController, Some(1), "fixture demand wake");
        let (tx, rx) = mpsc::sync_channel(1);
        send_runtime_wake(Some(&queue), &tx);
        let token = Arc::new(());
        let retained = Arc::downgrade(&token);
        drop(ProviderRuntimeDemand {
            queue: Some(queue.clone()),
            token: Some(token),
            wake: Some(tx.clone()),
        });
        assert_eq!(retained.strong_count(), 0);
        let q = live::snapshot()
            .unwrap()
            .queues
            .into_iter()
            .find(|q| q.byte_definition == "fixture demand wake")
            .unwrap();
        assert_eq!(
            (
                q.offered,
                q.enqueued,
                q.coalesced,
                q.rejected,
                q.depth,
                q.processed
            ),
            (2, 1, 1, 0, 1, 0)
        );
        rx.try_recv().unwrap();
        queue.dequeued(QueueBytes::Known(0));
        queue.outcome(pioneer_observability::queues::QueueOutcome::Processed, 1);
        drop(rx);
        drop(ProviderRuntimeDemand {
            queue: Some(queue.clone()),
            token: Some(Arc::new(())),
            wake: Some(tx.clone()),
        });
        let q = live::snapshot()
            .unwrap()
            .queues
            .into_iter()
            .find(|q| q.byte_definition == "fixture demand wake")
            .unwrap();
        assert_eq!(
            (
                q.offered,
                q.enqueued,
                q.coalesced,
                q.rejected,
                q.depth,
                q.processed
            ),
            (3, 1, 1, 1, 0, 1)
        );
        let pinned = queue.in_current_epoch();
        pioneer_observability::set_telemetry_enabled(false);
        pioneer_observability::set_telemetry_enabled(true);
        drop(ProviderRuntimeDemand {
            queue: Some(pinned),
            token: Some(Arc::new(())),
            wake: Some(tx),
        });
        assert!(live::snapshot().unwrap().queues.is_empty());
        pioneer_observability::set_telemetry_enabled(old);
    }

    use std::num::NonZeroUsize;
    fn authorized_core() -> ClientCore {
        use pioneer_protocol::*;
        let core = ClientCore::new();
        let role = AuthorizationRolePresentation {
            key: "admin".into(),
            display_name: "Administrator".into(),
            description: String::new(),
            built_in: false,
        };
        core.accept_authorization_projection(
            0,
            None,
            AuthorizationCapabilitySnapshot {
                schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                authorization_revision: 1,
                principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                role_key: "admin".into(),
                role,
                global: AuthorizationGlobalCapabilities {
                    can_manage_capabilities: true,
                    ..Default::default()
                },
                workspace: None,
                thread: None,
            },
        );
        core
    }
    fn runtime(id: &str) -> RuntimeSummary {
        serde_json::from_value(serde_json::json!({
            "runtime_id": id, "kind": "codex", "display_name": id,
            "enabled": true, "status": {"state":"ready"},
            "capabilities": pioneer_protocol::RuntimeCapabilities::default()
        }))
        .unwrap()
    }
    fn observe(core: &ClientCore, id: &str) -> RuntimeRequest {
        core.provider_runtime_intent(ProviderRuntimeIntent::Observe {
            workspace_id: id.into(),
        });
        request(core, id)
    }
    fn request(core: &ClientCore, id: &str) -> RuntimeRequest {
        core.provider_runtimes.lock().unwrap().scopes[id]
            .request
            .clone()
            .unwrap()
    }
    fn response(revision: u64, ids: &[&str]) -> CLIRuntimeListResponse {
        CLIRuntimeListResponse {
            revision,
            runtimes: ids.iter().map(|id| runtime(id)).collect(),
        }
    }
    fn replace_connection(core: &ClientCore, connection: u64) {
        let scope = ClientScope::Administration { workspace_id: None };
        let previous = core.snapshot(&scope).unwrap();
        let mut identity = (*previous
            .snapshot()
            .payload::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>()
            .unwrap())
        .clone();
        identity.connection_id = Some(connection);
        core.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(previous.revisions().scoped().get() + 1),
            Arc::new(identity),
            vec![],
        );
    }

    #[test]
    fn catalog_wait_follows_only_bounded_same_authorization_connections() {
        for replacements in [1, 2, 3] {
            let core = authorized_core();
            let mut waits = 0;
            let result =
                core.read_provider_runtime_request_with_wait("workspace", true, None, None, |_| {
                    waits += 1;
                    if waits <= replacements {
                        let old = request(&core, "workspace");
                        replace_connection(&core, waits as u64);
                        core.complete_provider_runtime(
                            &old,
                            Ok(response(100, &["stale"])),
                            Instant::now(),
                        );
                    } else {
                        let current = request(&core, "workspace");
                        core.complete_provider_runtime(
                            &current,
                            Ok(response(1, &["current"])),
                            Instant::now(),
                        );
                    }
                    Ok(())
                });
            if replacements <= 2 {
                assert_eq!(result.unwrap().runtimes[0].runtime_id, "current");
                assert_eq!(waits, replacements + 1);
            } else {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "provider_runtime_cancelled"
                );
                assert_eq!(waits, 3);
            }
        }
    }

    #[test]
    fn catalog_wait_cannot_cross_authorization_loss_or_repeat_a_probe() {
        for probe in [false, true] {
            let core = authorized_core();
            let mut waits = 0;
            let result = core.read_provider_runtime_request_with_wait(
                "workspace",
                true,
                probe.then_some(None),
                None,
                |_| {
                    waits += 1;
                    if probe {
                        replace_connection(&core, 9);
                    } else {
                        core.clear_authorization_projections();
                    }
                    Ok(())
                },
            );
            assert!(result.is_err());
            assert_eq!(waits, 1);
        }
    }

    #[test]
    fn explicit_probe_supersedes_snapshot_once_and_shares_the_existing_worker_generation() {
        let core = authorized_core();
        let first = observe(&core, "workspace");
        core.schedule_provider_runtime_refresh("workspace", Some("codex".into()))
            .unwrap();
        let probe = request(&core, "workspace");
        assert!(probe.generation > first.generation);
        assert_eq!(probe.refresh_runtime, Some(Some("codex".into())));
        core.schedule_provider_runtime_refresh("workspace", Some("codex".into()))
            .unwrap();
        assert_eq!(probe, request(&core, "workspace"));
        let loading = core.provider_runtime_snapshot("workspace").unwrap();
        core.complete_provider_runtime(
            &first,
            Ok(CLIRuntimeListResponse {
                revision: 1,
                runtimes: vec![runtime("old")],
            }),
            Instant::now(),
        );
        assert!(Arc::ptr_eq(
            &loading,
            &core.provider_runtime_snapshot("workspace").unwrap()
        ));
        core.provider_runtime_intent(ProviderRuntimeIntent::Release {
            workspace_id: "workspace".into(),
        });
        core.complete_provider_runtime(
            &probe,
            Ok(CLIRuntimeListResponse {
                revision: 2,
                runtimes: vec![runtime("codex")],
            }),
            Instant::now(),
        );
        assert_eq!(
            *core
                .provider_runtime_snapshot("workspace")
                .unwrap()
                .request(),
            ProviderRuntimeRequestState::Cancelled
        );
    }
    #[test]
    fn duplicate_runtime_identity_consumes_bounded_retry_budget_without_publishing_rows() {
        let core = authorized_core();
        let mut work = observe(&core, "workspace");
        for attempt in 0..RETRY_DELAYS.len() {
            core.complete_provider_runtime(
                &work,
                Ok(CLIRuntimeListResponse {
                    revision: 1,
                    runtimes: vec![runtime("same"), runtime("same")],
                }),
                Instant::now(),
            );
            let publication = core.provider_runtime_snapshot("workspace").unwrap();
            assert!(publication.runtimes().is_empty());
            if attempt + 1 < RETRY_DELAYS.len() {
                work = request(&core, "workspace");
                assert_eq!(work.attempt, attempt + 1);
            } else {
                assert_eq!(*publication.request(), ProviderRuntimeRequestState::Failed);
                assert!(
                    core.provider_runtime_request_for_test("workspace")
                        .is_none()
                );
            }
        }
    }
    #[test]
    fn equal_duplicate_and_wrong_scope_completions_publish_nothing() {
        let core = authorized_core();
        let work = observe(&core, "workspace");
        core.complete_provider_runtime(
            &work,
            Ok(response(1, &["first", "second"])),
            Instant::now(),
        );
        let snapshot = core.provider_runtime_snapshot("workspace").unwrap();
        let duplicate =
            core.complete_provider_runtime(&work, Ok(response(2, &["wrong"])), Instant::now());
        assert!(duplicate.changes().publications().is_empty());
        assert!(Arc::ptr_eq(
            &snapshot,
            &core.provider_runtime_snapshot("workspace").unwrap()
        ));
        let mut wrong = work.clone();
        wrong.workspace_id = "other".into();
        assert!(
            core.complete_provider_runtime(&wrong, Ok(response(3, &["wrong"])), Instant::now())
                .changes()
                .publications()
                .is_empty()
        );
        core.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
            workspace_id: "workspace".into(),
        });
        core.complete_provider_runtime(
            &request(&core, "workspace"),
            Ok(response(1, &["first", "second"])),
            Instant::now(),
        );
        let refreshed = core.provider_runtime_snapshot("workspace").unwrap();
        assert!(Arc::ptr_eq(&snapshot.runtimes[0], &refreshed.runtimes[0]));
        assert!(Arc::ptr_eq(&snapshot.runtimes[1], &refreshed.runtimes[1]));
    }
    #[test]
    fn stale_success_consumes_bounded_retry_budget_at_existing_cadence() {
        let core = authorized_core();
        observe(&core, "workspace");
        core.provider_runtimes
            .lock()
            .unwrap()
            .scopes
            .get_mut("workspace")
            .unwrap()
            .minimum_revision = 10;
        let now = Instant::now();
        for attempt in 0..4 {
            let work = request(&core, "workspace");
            assert_eq!(work.attempt, attempt);
            core.complete_provider_runtime(&work, Ok(response(9, &["stale"])), now);
            let owner = core.provider_runtimes.lock().unwrap();
            let scope = &owner.scopes["workspace"];
            if attempt < 3 {
                assert_eq!(scope.due, Some(now + RETRY_DELAYS[attempt + 1]));
            } else {
                assert!(scope.due.is_none());
                assert!(scope.request.is_none());
            }
        }
        assert_eq!(
            core.provider_runtime_snapshot("workspace")
                .unwrap()
                .request(),
            &ProviderRuntimeRequestState::Failed
        );
        assert!(
            core.provider_runtime_snapshot("workspace")
                .unwrap()
                .runtimes()
                .is_empty()
        );
    }
    #[test]
    fn demand_release_cancels_last_owner_and_late_completion() {
        let core = authorized_core();
        let first = observe(&core, "workspace");
        assert_eq!(first, observe(&core, "workspace"));
        core.provider_runtime_intent(ProviderRuntimeIntent::Release {
            workspace_id: "workspace".into(),
        });
        assert_eq!(request(&core, "workspace"), first);
        core.provider_runtime_intent(ProviderRuntimeIntent::Release {
            workspace_id: "workspace".into(),
        });
        let cancelled = core.provider_runtime_snapshot("workspace").unwrap();
        assert_eq!(cancelled.request(), &ProviderRuntimeRequestState::Cancelled);
        core.complete_provider_runtime(&first, Ok(response(50, &["late"])), Instant::now());
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.provider_runtime_snapshot("workspace").unwrap()
        ));
        let next = observe(&core, "workspace");
        assert!(next.generation > first.generation);
    }
    #[test]
    fn visible_refresh_does_not_escape_last_demand_cancellation() {
        let core = authorized_core();
        observe(&core, "workspace");
        core.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
            workspace_id: "workspace".into(),
        });
        core.provider_runtime_intent(ProviderRuntimeIntent::Release {
            workspace_id: "workspace".into(),
        });
        let owner = core.provider_runtimes.lock().unwrap();
        assert!(owner.scopes["workspace"].request.is_none());
        assert!(owner.scopes["workspace"].due.is_none());
    }
    #[test]
    fn provider_completion_does_not_publish_to_neighbor_scopes() {
        let core = Arc::new(authorized_core());
        let scopes = [
            ClientScope::Navigation,
            ClientScope::Administration { workspace_id: None },
            ClientScope::Thread {
                thread_id: "thread".into(),
            },
            ClientScope::Composer {
                thread_id: "thread".into(),
            },
            ClientScope::Settings,
        ];
        let neighbors: Vec<_> = scopes
            .into_iter()
            .map(|scope| core.subscribe(scope, NonZeroUsize::new(16).unwrap()))
            .collect();
        let work = observe(&core, "workspace");
        core.complete_provider_runtime(&work, Ok(response(1, &["runtime"])), Instant::now());
        for neighbor in neighbors {
            assert!(neighbor.try_next().is_none());
        }
    }
    #[test]
    fn retained_and_boundary_consumers_share_work_until_the_last_demand_is_dropped() {
        let core = authorized_core();
        let first = core.retain_provider_runtime("workspace");
        let work = request(&core, "workspace");
        let second = core.retain_provider_runtime("workspace");
        assert_eq!(observe(&core, "workspace"), work);
        drop(first);
        core.provider_runtime_intent(ProviderRuntimeIntent::Release {
            workspace_id: "workspace".into(),
        });
        assert_eq!(request(&core, "workspace"), work);
        core.complete_provider_runtime(&work, Ok(response(1, &["runtime"])), Instant::now());
        assert_eq!(
            core.provider_runtime_snapshot("workspace")
                .unwrap()
                .request(),
            &ProviderRuntimeRequestState::Ready
        );
        core.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
            workspace_id: "workspace".into(),
        });
        let late = request(&core, "workspace");
        drop(second);
        let before = core.provider_runtime_snapshot("workspace").unwrap();
        core.complete_provider_runtime(&late, Ok(response(2, &["late"])), Instant::now());
        assert!(Arc::ptr_eq(
            &before,
            &core.provider_runtime_snapshot("workspace").unwrap()
        ));
        let _replacement = core.retain_provider_runtime("workspace");
        assert_eq!(
            core.provider_runtime_snapshot("workspace")
                .unwrap()
                .request(),
            &ProviderRuntimeRequestState::Ready
        );
        assert!(
            core.provider_runtime_request_for_test("workspace")
                .is_none()
        );
    }
    #[test]
    fn another_consumer_does_not_hide_a_failed_refresh_of_retained_rows() {
        let core = authorized_core();
        let work = observe(&core, "workspace");
        core.complete_provider_runtime(&work, Ok(response(1, &["runtime"])), Instant::now());
        core.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
            workspace_id: "workspace".into(),
        });
        for _ in 0..4 {
            core.complete_provider_runtime(&request(&core, "workspace"), Err(()), Instant::now());
        }
        let failed = core.provider_runtime_snapshot("workspace").unwrap();
        assert_eq!(failed.request(), &ProviderRuntimeRequestState::Failed);
        let _other = core.retain_provider_runtime("workspace");
        assert!(Arc::ptr_eq(
            &failed,
            &core.provider_runtime_snapshot("workspace").unwrap()
        ));
        assert!(
            core.provider_runtime_request_for_test("workspace")
                .is_none()
        );
    }
    #[test]
    fn cli_partial_deltas_preserve_siblings_and_revision_gap_recovers_full_snapshot() {
        let core = authorized_core();
        let first = observe(&core, "workspace");
        core.complete_provider_runtime(&first, Ok(response(10, &["a", "b", "c"])), Instant::now());
        let before = core.provider_runtime_snapshot("workspace").unwrap();
        let notify = |revision, id: &str, removed, enabled| {
            let mut changed = runtime(id);
            changed.enabled = enabled;
            if !enabled {
                changed.status = pioneer_protocol::RuntimeStatus::Disabled;
            }
            core.reduce_provider_runtime_notification(
                &GatewayNotification::CLIRuntimeStatusChanged(
                    pioneer_protocol::CLIRuntimeStatusChangedNotification {
                        workspace_id: "workspace".into(),
                        revision,
                        runtime: changed,
                        removed,
                    },
                ),
            );
        };
        notify(11, "a", false, false);
        let disabled = core.provider_runtime_snapshot("workspace").unwrap();
        assert!(!disabled.runtimes[0].enabled);
        for index in [1, 2] {
            assert!(Arc::ptr_eq(
                &before.runtimes[index],
                &disabled.runtimes[index]
            ));
        }
        notify(12, "a", true, false);
        let removed = core.provider_runtime_snapshot("workspace").unwrap();
        assert_eq!(removed.runtimes.len(), 2);
        assert!(Arc::ptr_eq(&before.runtimes[1], &removed.runtimes[0]));
        // Revision 13 was lost. Applying 14 would create a mixed catalog;
        // the reducer retains rows and requests an authoritative snapshot.
        notify(14, "b", false, false);
        let loading = core.provider_runtime_snapshot("workspace").unwrap();
        assert_eq!(loading.request(), &ProviderRuntimeRequestState::Loading);
        assert_eq!(loading.runtimes.len(), removed.runtimes.len());
        for (retained, previous) in loading.runtimes.iter().zip(&removed.runtimes) {
            assert!(Arc::ptr_eq(retained, previous));
        }
        let recovery = request(&core, "workspace");
        core.complete_provider_runtime(&recovery, Ok(response(14, &["b", "c"])), Instant::now());
        let recovered = core.provider_runtime_snapshot("workspace").unwrap();
        assert_eq!(recovered.runtimes.len(), 2);
        notify(13, "a", false, true);
        assert!(Arc::ptr_eq(
            &recovered,
            &core.provider_runtime_snapshot("workspace").unwrap()
        ));
        assert!(
            core.provider_runtime_request_for_test("workspace")
                .is_none()
        );
    }

    fn invalidation(kind: &str) -> GatewayNotification {
        use pioneer_protocol::*;
        match kind {
            "account" => GatewayNotification::CLIRuntimeAccountUpdated(
                CLIRuntimeAccountUpdatedNotification {
                    workspace_id: "workspace".into(),
                    runtime_id: "a".into(),
                    kind: None,
                    account: None,
                    status: RuntimeStatus::Ready,
                },
            ),
            "apps" => {
                GatewayNotification::CLIRuntimeAppsChanged(CLIRuntimeAppsChangedNotification {
                    workspace_id: "workspace".into(),
                    runtime_id: "a".into(),
                    apps: vec![],
                    refreshed_at_unix_ms: None,
                })
            }
            "gap" | "zero" => {
                GatewayNotification::CLIRuntimeStatusChanged(CLIRuntimeStatusChangedNotification {
                    workspace_id: "workspace".into(),
                    revision: if kind == "gap" { 12 } else { 0 },
                    runtime: runtime("a"),
                    removed: false,
                })
            }
            _ => panic!("unknown invalidation fixture"),
        }
    }

    #[test]
    fn invalidations_fence_dispatched_reads_and_coalesce_before_the_next_dispatch() {
        for kind in ["account", "apps", "gap", "zero"] {
            for dispatched in [false, true] {
                let core = authorized_core();
                let first = observe(&core, "workspace");
                core.complete_provider_runtime(&first, Ok(response(10, &["a"])), Instant::now());
                let ready = core.provider_runtime_snapshot("workspace").unwrap();
                core.provider_runtime_intent(ProviderRuntimeIntent::Refresh {
                    workspace_id: "workspace".into(),
                });
                let old = request(&core, "workspace");
                if dispatched {
                    // The controller consumes due immediately before dispatch.
                    core.provider_runtimes
                        .lock()
                        .unwrap()
                        .scopes
                        .get_mut("workspace")
                        .unwrap()
                        .due = None;
                }
                core.observe_provider_runtime_notification(&invalidation(kind));
                let current = request(&core, "workspace");
                let loading = core.provider_runtime_snapshot("workspace").unwrap();
                assert_eq!(
                    loading.request(),
                    &ProviderRuntimeRequestState::Loading,
                    "{kind}"
                );
                assert!(Arc::ptr_eq(&ready.runtimes[0], &loading.runtimes[0]));
                for _ in 0..100 {
                    core.observe_provider_runtime_notification(&invalidation(kind));
                }
                assert_eq!(request(&core, "workspace"), current);
                if dispatched {
                    // Even a newer stream revision cannot bless a response
                    // whose request predates account/apps/unknown-gap evidence.
                    core.complete_provider_runtime(
                        &old,
                        Ok(response(100, &["stale"])),
                        Instant::now(),
                    );
                    assert!(Arc::ptr_eq(
                        &loading,
                        &core.provider_runtime_snapshot("workspace").unwrap()
                    ));
                } else {
                    assert_eq!(old, current);
                }
                core.complete_provider_runtime(
                    &current,
                    Ok(response(12, &["fresh"])),
                    Instant::now(),
                );
                let result = core.read_provider_runtimes("workspace", false).unwrap();
                assert_eq!(result.runtimes[0].runtime_id, "fresh");
                assert!(
                    core.provider_runtime_request_for_test("workspace")
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn undemanded_invalidation_retains_display_but_next_reader_loads_authoritative_rows() {
        for kind in ["account", "apps", "gap", "zero"] {
            let core = authorized_core();
            let first = observe(&core, "workspace");
            core.complete_provider_runtime(&first, Ok(response(10, &["a"])), Instant::now());
            core.provider_runtime_intent(ProviderRuntimeIntent::Release {
                workspace_id: "workspace".into(),
            });
            core.observe_provider_runtime_notification(&invalidation(kind));
            let retained = core.provider_runtime_snapshot("workspace").unwrap();
            assert_eq!(retained.request(), &ProviderRuntimeRequestState::Loading);
            assert_eq!(retained.runtimes[0].runtime_id, "a");
            assert!(
                core.provider_runtime_request_for_test("workspace")
                    .is_none()
            );
            let mut reads = 0;
            let result = core
                .read_provider_runtime_request_with_wait("workspace", false, None, None, |_| {
                    reads += 1;
                    core.complete_provider_runtime(
                        &request(&core, "workspace"),
                        Ok(response(12, &["fresh"])),
                        Instant::now(),
                    );
                    Ok(())
                })
                .unwrap();
            assert_eq!(reads, 1, "{kind}");
            assert_eq!(result.runtimes[0].runtime_id, "fresh");
        }
    }

    #[test]
    fn invalidation_preserves_retry_budget_and_does_not_repeat_an_inflight_probe() {
        let core = authorized_core();
        let first = observe(&core, "workspace");
        core.complete_provider_runtime(&first, Err(()), Instant::now());
        let retry = request(&core, "workspace");
        let due = core.provider_runtimes.lock().unwrap().scopes["workspace"].due;
        core.observe_provider_runtime_notification(&invalidation("account"));
        assert_eq!(request(&core, "workspace"), retry);
        assert_eq!(
            core.provider_runtimes.lock().unwrap().scopes["workspace"].due,
            due
        );
        core.provider_runtimes
            .lock()
            .unwrap()
            .scopes
            .get_mut("workspace")
            .unwrap()
            .due = None;
        core.observe_provider_runtime_notification(&invalidation("apps"));
        let fenced_retry = request(&core, "workspace");
        assert_eq!(fenced_retry.attempt, retry.attempt);
        assert_ne!(fenced_retry.generation, retry.generation);
        for attempt in fenced_retry.attempt..RETRY_DELAYS.len() {
            core.observe_provider_runtime_notification(&invalidation("account"));
            let work = request(&core, "workspace");
            assert_eq!(work.attempt, attempt);
            core.complete_provider_runtime(&work, Err(()), Instant::now());
        }
        assert_eq!(
            core.provider_runtime_snapshot("workspace")
                .unwrap()
                .request(),
            &ProviderRuntimeRequestState::Failed
        );
        assert!(
            core.provider_runtime_request_for_test("workspace")
                .is_none()
        );
        core.schedule_provider_runtime_refresh("workspace", Some("a".into()))
            .unwrap();
        let probe = request(&core, "workspace");
        core.provider_runtimes
            .lock()
            .unwrap()
            .scopes
            .get_mut("workspace")
            .unwrap()
            .due = None;
        core.observe_provider_runtime_notification(&invalidation("apps"));
        let list = request(&core, "workspace");
        assert_eq!(list.refresh_runtime, None);
        core.complete_provider_runtime(&probe, Ok(response(50, &["probe"])), Instant::now());
        assert_eq!(
            core.provider_runtime_snapshot("workspace")
                .unwrap()
                .request(),
            &ProviderRuntimeRequestState::Loading
        );
        core.complete_provider_runtime(&list, Ok(response(51, &["list"])), Instant::now());
        assert_eq!(
            core.read_provider_runtimes("workspace", false)
                .unwrap()
                .runtimes[0]
                .runtime_id,
            "list"
        );
    }

    #[test]
    fn budget_cancel_drops_only_the_send_waiter_while_visible_consumer_keeps_the_load() {
        use crate::rpc::{JsonRpcRequestTransport, JsonRpcResponseSender, RequestBudget};
        struct RuntimeReplies(mpsc::Sender<JsonRpcResponseSender>);
        impl JsonRpcRequestTransport for RuntimeReplies {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                reply: JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: pioneer_protocol::JsonRpcRequest =
                    serde_json::from_str(&payload).unwrap();
                assert_eq!(
                    request.method,
                    pioneer_protocol::constants::methods::CLI_RUNTIME_LIST
                );
                self.0.send(reply).map_err(|error| error.to_string())
            }
        }
        struct Cleanup(Arc<ClientCore>, RequestBudget);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                self.1.cancel();
                self.0.shutdown();
            }
        }
        let core = crate::catalog_test_support::client();
        let budget = RequestBudget::new(Instant::now() + Duration::from_secs(60));
        let _cleanup = Cleanup(core.clone(), budget.clone());
        core.transport_runtime().inject_test_event(
            crate::transport::ws::GatewayWsEvent::Connected {
                connection_id: 7,
                endpoint_id: "endpoint".into(),
                endpoint_name: "Synthetic".into(),
                gateway_base_url: pioneer_protocol::GatewayBaseUrl::parse_presentation(
                    "https://synthetic.invalid",
                )
                .unwrap(),
            },
        );
        let (replies, received) = mpsc::channel();
        *core
            .transport_runtime()
            .ws_command_sender()
            .test_requests
            .lock()
            .unwrap() = Some(Arc::new(RuntimeReplies(replies)));
        let visible = core.retain_provider_runtime("workspace");
        core.start_provider_runtime_controller();
        let reply = received.recv_timeout(Duration::from_secs(5)).unwrap();
        let load = request(&core, "workspace");
        let (finished, result) = mpsc::channel();
        let waiter_core = core.clone();
        let waiter_budget = budget.clone();
        let waiter = std::thread::spawn(move || {
            finished
                .send(waiter_core.read_provider_runtimes_with_budget(
                    "workspace",
                    false,
                    &waiter_budget,
                ))
                .unwrap();
        });
        let registration_deadline = Instant::now() + Duration::from_secs(5);
        while core.provider_runtimes.lock().unwrap().scopes["workspace"]
            .listeners
            .is_empty()
        {
            assert!(
                Instant::now() < registration_deadline,
                "Send waiter did not register"
            );
            std::thread::yield_now();
        }
        budget.cancel();
        assert!(
            result
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .is_err()
        );
        waiter.join().unwrap();
        assert_eq!(request(&core, "workspace"), load);
        reply
            .send(Ok(
                serde_json::to_value(response(1, &["surviving"])).unwrap()
            ))
            .unwrap();
        let rows = core.read_provider_runtimes("workspace", false).unwrap();
        assert_eq!(rows.runtimes[0].runtime_id, "surviving");
        assert!(
            received.try_recv().is_err(),
            "cancel must not start a replacement list"
        );
        drop(visible);
    }

    #[test]
    fn access_invalidation_drops_data_and_rejects_old_request() {
        let core = authorized_core();
        let work = observe(&core, "workspace");
        core.clear_authorization_projections();
        core.complete_provider_runtime(&work, Ok(response(100, &["protected"])), Instant::now());
        assert!(core.provider_runtime_snapshot("workspace").is_none());
        core.provider_runtime_intent(ProviderRuntimeIntent::Observe {
            workspace_id: "workspace".into(),
        });
        assert!(
            core.provider_runtime_request_for_test("workspace")
                .is_none()
        );
    }
}

#[cfg(any(test, feature = "test-support"))]
impl ClientCore {
    pub(crate) fn provider_runtime_request_for_test(
        &self,
        workspace: &str,
    ) -> Option<RuntimeRequest> {
        self.provider_runtimes
            .lock()
            .unwrap()
            .scopes
            .get(workspace)?
            .request
            .clone()
    }
    pub(crate) fn complete_provider_runtime_for_test(
        &self,
        request: RuntimeRequest,
        result: Result<CLIRuntimeListResponse, ()>,
    ) {
        self.complete_provider_runtime(&request, result, Instant::now());
    }
}

// Fixture completions enter the existing owner; no worker or extra demand is started.
#[cfg(feature = "test-support")]
impl ClientMutationAuthority {
    pub fn complete_provider_runtime_for_test(
        &self,
        core: &ClientCore,
        workspace: &str,
        result: Result<CLIRuntimeListResponse, ()>,
    ) {
        let request = core
            .provider_runtime_request_for_test(workspace)
            .expect("pending runtime fixture request");
        core.complete_provider_runtime_for_test(request, result);
    }

    pub fn provider_runtime_status_changed_for_test(
        &self,
        core: &ClientCore,
        update: pioneer_protocol::CLIRuntimeStatusChangedNotification,
    ) {
        core.observe_provider_runtime_notification(&GatewayNotification::CLIRuntimeStatusChanged(
            update,
        ));
    }
}
