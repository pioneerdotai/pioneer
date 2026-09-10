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
    token: Option<Arc<()>>,
    wake: Option<mpsc::SyncSender<()>>,
}
impl Drop for ProviderRuntimeDemand {
    fn drop(&mut self) {
        self.token.take();
        if let Some(wake) = &self.wake {
            let _ = wake.try_send(());
        }
    }
}
struct RuntimeScope {
    publication: Arc<ProviderRuntimePublication>,
    auth: (u64, u64, Option<u64>),
    source_revision: u64,
    minimum_revision: u64,
    loaded: bool,
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
            let _ = wake.try_send(());
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
        anyhow::ensure!(
            !workspace.trim().is_empty() && !self.is_stopped(),
            "provider_runtime_unavailable"
        );
        let epoch = self.provider_runtime_epoch();
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
            anyhow::ensure!(
                !self.is_stopped() && self.provider_runtime_epoch() == epoch,
                "provider_runtime_cancelled"
            );
            {
                let owner = self
                    .provider_runtimes
                    .lock()
                    .expect("provider runtimes poisoned");
                let scope = owner
                    .scopes
                    .get(workspace)
                    .ok_or_else(|| anyhow::anyhow!("provider_runtime_cancelled"))?;
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
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => anyhow::bail!("provider_runtime_cancelled"),
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
        let revision = next.revision;
        let workspace_id = next.workspace_id.clone();
        scope.publication = Arc::new(next);
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
        let wake = self
            .provider_runtimes
            .lock()
            .expect("provider runtimes poisoned")
            .wake
            .clone();
        ProviderRuntimeDemand {
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
        if scope.auth != auth {
            scope.auth = auth;
            scope.loaded = false;
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
            let mut next = (*scope.publication).clone();
            next.runtimes.clear();
            next.request = ProviderRuntimeRequestState::Cancelled;
            return self.publish_provider_runtime(scope, next);
        }
        if scope.request.is_some() {
            return self.navigation_outcome(ClientTransitionOutcome::Noop);
        }
        if observe && scope.publication.request == ProviderRuntimeRequestState::Failed {
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
        if !scope.loaded {
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
            scope.source_revision = 0;
            scope.minimum_revision = 0;
            let mut next = (*scope.publication).clone();
            next.runtimes.clear();
            next.request = ProviderRuntimeRequestState::Cancelled;
            scope.publication = Arc::new(next);
        }
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
            if owner.scopes.get(workspace).is_some_and(|scope| {
                scope.auth == auth && scope.demanded() && scope.request.is_none()
            }) {
                owner.schedule(workspace, auth);
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
            if scope.demanded() && scope.request.is_none() {
                let auth = scope.auth;
                owner.schedule(&update.workspace_id, auth);
            }
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
        let weak = Arc::downgrade(self);
        let mut owner = self
            .provider_runtimes
            .lock()
            .expect("provider runtimes poisoned");
        owner.wake = Some(sender);
        owner.task = Some(
            std::thread::Builder::new()
                .name("client-provider-runtimes".into())
                .spawn(move || {
                    loop {
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
                                continue;
                            }
                            let sender = core.transport_runtime().ws_command_sender();
                            drop(core);
                            let result = if let Some(runtime_id) = &request.refresh_runtime {
                                sender
                                    .cli_runtime_refresh(
                                        pioneer_protocol::CLIRuntimeRefreshParams {
                                            workspace_id: request.workspace_id.clone(),
                                            runtime_id: runtime_id.clone(),
                                        },
                                    )
                                    .map(|response| CLIRuntimeListResponse {
                                        revision: response.revision,
                                        runtimes: response.runtimes,
                                    })
                            } else {
                                sender.cli_runtime_list(CLIRuntimeListParams {
                                    workspace_id: request.workspace_id.clone(),
                                })
                            }
                            .map_err(|_| ());
                            let Some(core) = weak.upgrade() else {
                                break;
                            };
                            core.complete_provider_runtime(&request, result, Instant::now());
                        } else {
                            drop(core);
                            match delay {
                                Some(delay) => {
                                    if matches!(
                                        receiver.recv_timeout(delay),
                                        Err(mpsc::RecvTimeoutError::Disconnected)
                                    ) {
                                        break;
                                    }
                                }
                                None => {
                                    if receiver.recv().is_err() {
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
