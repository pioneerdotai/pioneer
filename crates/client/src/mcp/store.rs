//! Immutable server catalogs and details owned by the process-local Client.
use crate::{core::*, request_state::poll::PollRequest};
use pioneer_protocol::{McpListItem, McpServerDetailsResponse};
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak, mpsc},
    time::Duration,
};

const POLL_INTERVAL: Duration = Duration::from_secs(20);

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpLoadState {
    #[default]
    Idle,
    Loading,
    Ready,
    Failed,
    Cancelled,
    Forbidden,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct McpCatalogPublication {
    workspace_id: String,
    revision: u64,
    servers: Vec<Arc<McpListItem>>,
    request: McpLoadState,
}
impl McpCatalogPublication {
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn servers(&self) -> &[Arc<McpListItem>] {
        &self.servers
    }
    pub fn request(&self) -> McpLoadState {
        self.request
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct McpDetailsPublication {
    workspace_id: String,
    server_id: String,
    revision: u64,
    details: Option<Arc<McpServerDetailsResponse>>,
    request: McpLoadState,
}
impl McpDetailsPublication {
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }
    pub fn server_id(&self) -> &str {
        &self.server_id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn details(&self) -> Option<&Arc<McpServerDetailsResponse>> {
        self.details.as_ref()
    }
    pub fn request(&self) -> McpLoadState {
        self.request
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadRequest {
    workspace: String,
    server: Option<String>,
    generation: u64,
    epoch: (u64, u64, Option<u64>),
}
struct Details {
    publication: Arc<McpDetailsPublication>,
    request: Option<ReadRequest>,
    queued: bool,
    waiters: BTreeMap<u64, mpsc::SyncSender<Arc<McpDetailsPublication>>>,
}
struct Catalog {
    version: u64,
    publication: Arc<McpCatalogPublication>,
    details: BTreeMap<String, Details>,
    poll: PollRequest,
    request: Option<ReadRequest>,
    epoch: (u64, u64, Option<u64>),
    consumers: BTreeMap<u64, Option<String>>,
    waiters: BTreeMap<u64, mpsc::SyncSender<Arc<McpCatalogPublication>>>,
}
#[derive(Default)]
pub(crate) struct McpStore {
    binding_gate: Arc<std::sync::Mutex<()>>,
    suspended: bool,
    binding_demands: std::collections::HashMap<ClientScope, u64>,
    catalogs: BTreeMap<String, Catalog>,
    generation: u64,
    next_consumer: u64,
    wake: Option<mpsc::SyncSender<()>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl McpStore {
    pub(crate) fn stop(&mut self) {
        self.wake.take();
        self.catalogs.clear();
    }
    fn wake(&self) {
        if let Some(wake) = &self.wake {
            let _ = wake.try_send(());
        }
    }
}
impl Drop for McpStore {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
/// An active consumer's demand. Warm views and backgrounded consumers release it.
/// Several leases in one process share a single recurring read owner.
pub struct McpDemand {
    core: Weak<ClientCore>,
    workspace: String,
    identity: u64,
}
impl Drop for McpDemand {
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            core.release_mcp_demand(&self.workspace, self.identity);
        }
    }
}
pub struct McpRead<T> {
    _demand: McpDemand,
    receiver: mpsc::Receiver<Arc<T>>,
}
impl<T> McpRead<T> {
    pub fn wait_while(self, current: impl Fn() -> bool) -> anyhow::Result<Arc<T>> {
        loop {
            anyhow::ensure!(current(), "mcp_read_cancelled");
            match self.receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(p) => return Ok(p),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => anyhow::bail!("mcp_read_cancelled"),
            }
        }
    }
}
impl ClientCore {
    pub(crate) fn mcp_binding_demand_changed(&self, scope: &ClientScope, demand: ClientDemand) {
        let gate = self
            .mcp_store
            .lock()
            .expect("catalog store poisoned")
            .binding_gate
            .clone();
        let _binding = gate.lock().expect("catalog binding demand poisoned");
        let demand = self.current_scope_demand(scope).unwrap_or(demand);
        let (workspace, server) = match scope {
            ClientScope::Mcp {
                workspace_id: Some(w),
            } => (w.as_str(), None),
            ClientScope::McpDetails {
                workspace_id,
                server_id,
            } => (workspace_id.as_str(), Some(server_id.as_str())),
            _ => return,
        };
        if demand == ClientDemand::Suspended {
            let identity = self
                .mcp_store
                .lock()
                .expect("MCP store poisoned")
                .binding_demands
                .remove(scope);
            if let Some(identity) = identity {
                self.release_mcp_demand(workspace, identity);
            }
            return;
        }
        if self
            .mcp_store
            .lock()
            .expect("MCP store poisoned")
            .binding_demands
            .contains_key(scope)
        {
            return;
        }
        // The binding registry owns this lease token; it releases it synchronously on suspension.
        let lease = self.acquire_mcp_demand_kind(workspace, server, true, Weak::new());
        self.mcp_store
            .lock()
            .expect("MCP store poisoned")
            .binding_demands
            .insert(scope.clone(), lease.identity);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn mcp_demand_count_for_test(&self, workspace: &str) -> usize {
        self.mcp_store
            .lock()
            .unwrap()
            .catalogs
            .get(workspace)
            .map_or(0, |c| c.consumers.len())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn accept_mcp_catalog_for_test(
        self: &Arc<Self>,
        workspace: &str,
        response: pioneer_protocol::McpListResponse,
    ) {
        let _demand = self.acquire_mcp_demand(workspace, None);
        self.refresh_mcp(workspace);
        let request = self.next_mcp_read().expect("synthetic catalog demand");
        self.complete_mcp_catalog(request, Ok(response));
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn accept_mcp_details_for_test(
        self: &Arc<Self>,
        workspace: &str,
        server: &str,
        response: anyhow::Result<McpServerDetailsResponse>,
    ) {
        let _demand = self.acquire_mcp_demand(workspace, Some(server));
        self.refresh_mcp(workspace);
        let request = self.next_mcp_read().expect("synthetic detail demand");
        assert_eq!(request.server.as_deref(), Some(server));
        self.complete_mcp_details(request, response);
    }

    pub fn read_mcp_catalog(self: &Arc<Self>, workspace: &str) -> McpRead<McpCatalogPublication> {
        let demand = self.acquire_mcp_demand(workspace, None);
        let (sender, receiver) = mpsc::sync_channel(1);
        if self.provider_runtime_epoch().2.is_none() || !self.mcp_allowed(workspace) {
            drop(sender);
            return McpRead {
                _demand: demand,
                receiver,
            };
        }
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        let catalog = owner.catalogs.get_mut(workspace).expect("demanded catalog");
        catalog.waiters.insert(demand.identity, sender);
        catalog.poll.refresh(self.timeline_started.elapsed());
        owner.wake();
        McpRead {
            _demand: demand,
            receiver,
        }
    }
    pub fn read_mcp_details(
        self: &Arc<Self>,
        workspace: &str,
        server: &str,
    ) -> McpRead<McpDetailsPublication> {
        let demand =
            self.acquire_mcp_demand_kind(workspace, Some(server), false, Arc::downgrade(self));
        let (sender, receiver) = mpsc::sync_channel(1);
        if self.provider_runtime_epoch().2.is_none() || !self.mcp_allowed(workspace) {
            drop(sender);
            return McpRead {
                _demand: demand,
                receiver,
            };
        }
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        let detail = owner
            .catalogs
            .get_mut(workspace)
            .unwrap()
            .details
            .get_mut(server)
            .unwrap();
        detail.waiters.insert(demand.identity, sender);
        if detail.request.is_none() {
            detail.queued = true;
        }
        owner.wake();
        McpRead {
            _demand: demand,
            receiver,
        }
    }

    pub fn mcp_catalog_snapshot(&self, workspace: &str) -> Option<Arc<McpCatalogPublication>> {
        self.snapshot(&ClientScope::Mcp {
            workspace_id: Some(workspace.into()),
        })
        .and_then(|p| p.snapshot().payload())
    }
    pub fn mcp_details_snapshot(
        &self,
        workspace: &str,
        server: &str,
    ) -> Option<Arc<McpDetailsPublication>> {
        self.snapshot(&ClientScope::McpDetails {
            workspace_id: workspace.into(),
            server_id: server.into(),
        })
        .and_then(|p| p.snapshot().payload())
    }
    fn mcp_allowed(&self, workspace: &str) -> bool {
        !self.is_stopped()
            && self
                .authorization_snapshot(Some(workspace), None)
                .or_else(|| self.authorization_snapshot(None, None))
                .is_some_and(|p| {
                    let capabilities =
                        crate::authorization::principal_presentation_capabilities(&p);
                    capabilities.can_use_mcp || capabilities.can_manage_capabilities
                })
    }
    pub fn acquire_mcp_demand(
        self: &Arc<Self>,
        workspace: &str,
        server: Option<&str>,
    ) -> McpDemand {
        self.acquire_mcp_demand_kind(workspace, server, true, Arc::downgrade(self))
    }
    fn acquire_mcp_demand_kind(
        &self,
        workspace: &str,
        server: Option<&str>,
        recurring: bool,
        core: Weak<ClientCore>,
    ) -> McpDemand {
        let now = self.timeline_started.elapsed();
        let epoch = self.provider_runtime_epoch();
        let allowed = self.mcp_allowed(workspace);
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        owner.next_consumer = owner
            .next_consumer
            .checked_add(1)
            .expect("MCP consumer identity exhausted");
        let identity = owner.next_consumer;
        let catalog = owner
            .catalogs
            .entry(workspace.into())
            .or_insert_with(|| Catalog {
                version: 0,
                publication: Arc::new(McpCatalogPublication {
                    workspace_id: workspace.into(),
                    revision: 0,
                    servers: vec![],
                    request: McpLoadState::Idle,
                }),
                details: BTreeMap::new(),
                poll: PollRequest::new(POLL_INTERVAL),
                request: None,
                epoch,
                consumers: BTreeMap::new(),
                waiters: BTreeMap::new(),
            });
        catalog
            .poll
            .set_connected(allowed && epoch.2.is_some(), now);
        if recurring {
            catalog.poll.acquire(identity, now);
        }
        let first_detail_consumer = server.is_some_and(|server| {
            !catalog
                .consumers
                .values()
                .any(|id| id.as_deref() == Some(server))
        });
        catalog
            .consumers
            .insert(identity, server.map(str::to_owned));
        if let Some(server) = server {
            let detail = catalog
                .details
                .entry(server.into())
                .or_insert_with(|| Details {
                    publication: Arc::new(McpDetailsPublication {
                        workspace_id: workspace.into(),
                        server_id: server.into(),
                        revision: 0,
                        details: None,
                        request: McpLoadState::Idle,
                    }),
                    request: None,
                    queued: true,
                    waiters: BTreeMap::new(),
                });
            if first_detail_consumer && detail.request.is_none() {
                detail.queued = true;
            }
        }
        owner.wake();
        McpDemand {
            core,
            workspace: workspace.into(),
            identity,
        }
    }
    fn release_mcp_demand(&self, workspace: &str, identity: u64) {
        let mut owner = self
            .mcp_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(catalog) = owner.catalogs.get_mut(workspace) {
            catalog.consumers.remove(&identity);
            catalog.waiters.remove(&identity);
            for detail in catalog.details.values_mut() {
                detail.waiters.remove(&identity);
            }
            catalog.poll.release(identity);
            if catalog.consumers.is_empty() {
                if catalog.request.take().is_some() {
                    let mut next = (*catalog.publication).clone();
                    if next.request == McpLoadState::Loading {
                        next.request = McpLoadState::Cancelled;
                    }
                    self.publish_mcp_catalog(catalog, next);
                }
            }
            for (id, detail) in &mut catalog.details {
                if !catalog
                    .consumers
                    .values()
                    .any(|server| server.as_ref() == Some(id))
                {
                    detail.queued = false;
                    if detail.request.take().is_some() {
                        let mut next = (*detail.publication).clone();
                        if next.request == McpLoadState::Loading {
                            next.request = McpLoadState::Cancelled;
                        }
                        self.publish_mcp_details(detail, next);
                    }
                }
            }
        }
        owner.wake();
    }
    pub(crate) fn fence_mcp_reads_after_action(&self, workspace: &str, target: &str) {
        let now = self.timeline_started.elapsed();
        let connected = self.provider_runtime_epoch().2.is_some() && self.mcp_allowed(workspace);
        let mut owner = self.mcp_store.lock().expect("catalog store poisoned");
        if let Some(catalog) = owner.catalogs.get_mut(workspace) {
            catalog.request = None;
            catalog.poll.set_connected(false, now);
            catalog.poll.set_connected(connected, now);
            for (id, detail) in &mut catalog.details {
                if target == "configuration" || id == target {
                    detail.request = None;
                    detail.queued = catalog
                        .consumers
                        .values()
                        .any(|server| server.as_ref() == Some(id));
                }
            }
        }
        owner.wake();
    }
    pub fn refresh_mcp(&self, workspace: &str) {
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        if let Some(catalog) = owner.catalogs.get_mut(workspace) {
            catalog.poll.refresh(self.timeline_started.elapsed());
            for (id, detail) in &mut catalog.details {
                if catalog
                    .consumers
                    .values()
                    .any(|server| server.as_ref() == Some(id))
                    && detail.request.is_none()
                {
                    detail.queued = true;
                }
            }
        }
        owner.wake();
    }
    fn publish_mcp_catalog(&self, catalog: &mut Catalog, mut next: McpCatalogPublication) {
        next.revision = catalog.publication.revision;
        if *catalog.publication == next {
            return;
        }
        let scope = ClientScope::Mcp {
            workspace_id: Some(next.workspace_id.clone()),
        };
        next.revision = next
            .revision
            .max(
                self.snapshot(&scope)
                    .map_or(0, |p| p.revisions().scoped().get()),
            )
            .checked_add(1)
            .expect("MCP catalog revision exhausted");
        catalog.publication = Arc::new(next);
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(catalog.publication.revision),
            catalog.publication.clone(),
            vec![],
        );
    }
    fn publish_mcp_details(&self, detail: &mut Details, mut next: McpDetailsPublication) {
        next.revision = detail.publication.revision;
        if *detail.publication == next {
            return;
        }
        let scope = ClientScope::McpDetails {
            workspace_id: next.workspace_id.clone(),
            server_id: next.server_id.clone(),
        };
        next.revision = next
            .revision
            .max(
                self.snapshot(&scope)
                    .map_or(0, |p| p.revisions().scoped().get()),
            )
            .checked_add(1)
            .expect("MCP detail revision exhausted");
        detail.publication = Arc::new(next);
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(detail.publication.revision),
            detail.publication.clone(),
            vec![],
        );
    }
    pub(crate) fn apply_mcp_policy(
        &self,
        workspace: &str,
        id: &str,
        enabled: bool,
        implicit: bool,
    ) {
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        let Some(catalog) = owner.catalogs.get_mut(workspace) else {
            return;
        };
        let mut next = (*catalog.publication).clone();
        let Some(row) = next.servers.iter_mut().find(|s| s.id == id) else {
            return;
        };
        let name = row.name.clone();
        let mut changed = (**row).clone();
        super::actions::apply_local_mcp_policy(
            std::slice::from_mut(&mut changed),
            &mut None,
            &name,
            enabled,
            implicit,
        );
        if **row != changed {
            *row = Arc::new(changed);
        }
        self.publish_mcp_catalog(catalog, next);
        if let Some(detail) = catalog.details.get_mut(id) {
            let mut next = (*detail.publication).clone();
            let mut changed = next.details.as_deref().cloned();
            super::actions::apply_local_mcp_policy(&mut [], &mut changed, &name, enabled, implicit);
            if next.details.as_deref() != changed.as_ref() {
                next.details = changed.map(Arc::new);
            }
            self.publish_mcp_details(detail, next);
        }
    }
    pub(crate) fn observe_mcp_notification(
        &self,
        notification: &pioneer_protocol::GatewayNotification,
    ) -> bool {
        use pioneer_protocol::GatewayNotification;
        let workspace = match notification {
            GatewayNotification::McpChanged(n) => {
                self.refresh_mcp(&n.workspace_id);
                return true;
            }
            GatewayNotification::McpServerStatusChanged(n) => &n.workspace_id,
            GatewayNotification::McpServerCatalogChanged(n) => &n.workspace_id,
            _ => return false,
        };
        if !self.mcp_allowed(workspace) {
            return true;
        }
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        let Some(catalog) = owner.catalogs.get_mut(workspace) else {
            return true;
        };
        if catalog.epoch != self.provider_runtime_epoch() {
            return true;
        }
        let mut next = (*catalog.publication).clone();
        match notification {
            GatewayNotification::McpServerStatusChanged(n) => {
                if n.snapshot_version < catalog.version {
                    return true;
                }
                catalog.version = n.snapshot_version;
                if let Some(row) = next.servers.iter_mut().find(|s| s.id == n.server.id) {
                    let mut changed = (**row).clone();
                    super::notifications::apply_mcp_server_status_changed_to_catalog(
                        std::slice::from_mut(&mut changed),
                        n,
                    );
                    if **row != changed {
                        *row = Arc::new(changed);
                    }
                }
                if let Some(detail) = catalog.details.get_mut(&n.server.id) {
                    let mut next = (*detail.publication).clone();
                    if let Some(value) = &next.details {
                        let mut changed = (**value).clone();
                        super::notifications::apply_mcp_server_status_changed_to_details(
                            &mut changed,
                            n,
                        );
                        if **value != changed {
                            next.details = Some(Arc::new(changed));
                        }
                    }
                    self.publish_mcp_details(detail, next);
                }
            }
            GatewayNotification::McpServerCatalogChanged(n) => {
                if n.snapshot_version < catalog.version {
                    return true;
                }
                catalog.version = n.snapshot_version;
                if let Some(row) = next.servers.iter_mut().find(|s| s.id == n.server_id) {
                    let mut changed = (**row).clone();
                    super::notifications::apply_mcp_server_catalog_changed_to_catalog(
                        std::slice::from_mut(&mut changed),
                        n,
                    );
                    if **row != changed {
                        *row = Arc::new(changed);
                    }
                }
                if let Some(detail) = catalog.details.get_mut(&n.server_id) {
                    if detail.request.is_none() {
                        detail.queued = true;
                    }
                }
            }
            _ => {}
        }
        self.publish_mcp_catalog(catalog, next);
        owner.wake();
        true
    }
    pub(crate) fn invalidate_mcp(&self) {
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        owner.suspended = true;
        for catalog in owner.catalogs.values_mut() {
            catalog
                .poll
                .set_connected(false, self.timeline_started.elapsed());
            catalog.request = None;
            catalog.waiters.clear();
            let mut next = (*catalog.publication).clone();
            next.servers.clear();
            next.request = McpLoadState::Cancelled;
            self.publish_mcp_catalog(catalog, next);
            for detail in catalog.details.values_mut() {
                detail.request = None;
                detail.waiters.clear();
                detail.queued = false;
                let mut next = (*detail.publication).clone();
                next.details = None;
                next.request = McpLoadState::Cancelled;
                self.publish_mcp_details(detail, next);
            }
        }
        owner.wake();
    }
    fn next_mcp_read(&self) -> Option<ReadRequest> {
        let epoch = self.provider_runtime_epoch();
        let now = self.timeline_started.elapsed();
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        if owner.suspended {
            return None;
        }
        let detail_key = owner.catalogs.iter().find_map(|(workspace, catalog)| {
            if catalog.epoch != epoch || epoch.2.is_none() {
                return None;
            }
            catalog
                .details
                .iter()
                .find(|(id, detail)| {
                    detail.queued
                        && detail.request.is_none()
                        && catalog
                            .consumers
                            .values()
                            .any(|server| server.as_ref() == Some(id))
                })
                .map(|(id, _)| (workspace.clone(), id.clone()))
        });
        if let Some((workspace, server)) = detail_key {
            if !self.mcp_allowed(&workspace) {
                return None;
            }
            owner.generation = owner
                .generation
                .checked_add(1)
                .expect("MCP read generation exhausted");
            let request = ReadRequest {
                workspace: workspace.clone(),
                server: Some(server.clone()),
                generation: owner.generation,
                epoch,
            };
            let detail = owner
                .catalogs
                .get_mut(&workspace)
                .unwrap()
                .details
                .get_mut(&server)
                .unwrap();
            detail.queued = false;
            detail.request = Some(request.clone());
            if detail.publication.details.is_none() {
                let mut next = (*detail.publication).clone();
                next.request = McpLoadState::Loading;
                self.publish_mcp_details(detail, next);
            }
            return Some(request);
        }
        let workspace = owner
            .catalogs
            .keys()
            .find(|workspace| {
                let catalog = &owner.catalogs[*workspace];
                !catalog.consumers.is_empty()
                    && catalog.request.is_none()
                    && (catalog.epoch != epoch || catalog.poll.due().is_some_and(|due| due <= now))
            })?
            .clone();
        let connected = epoch.2.is_some() && self.mcp_allowed(&workspace);
        let catalog = owner.catalogs.get_mut(&workspace).unwrap();
        if catalog.epoch != epoch {
            catalog.poll.set_connected(false, now);
            catalog.epoch = epoch;
        }
        catalog.poll.set_connected(connected, now);
        let generation = catalog.poll.claim(now)?;
        let request = ReadRequest {
            workspace,
            server: None,
            generation,
            epoch,
        };
        catalog.request = Some(request.clone());
        if catalog.publication.servers.is_empty()
            && catalog.publication.request != McpLoadState::Ready
        {
            let mut next = (*catalog.publication).clone();
            next.request = McpLoadState::Loading;
            self.publish_mcp_catalog(catalog, next);
        }
        Some(request)
    }
    fn complete_mcp_catalog(
        &self,
        request: ReadRequest,
        response: anyhow::Result<pioneer_protocol::McpListResponse>,
    ) {
        if self.provider_runtime_epoch() != request.epoch || !self.mcp_allowed(&request.workspace) {
            return;
        }
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        let Some(catalog) = owner.catalogs.get_mut(&request.workspace) else {
            return;
        };
        if catalog.request.as_ref() != Some(&request)
            || !catalog.poll.is_current(request.generation)
        {
            return;
        }
        let response = response.and_then(|response| {
            let mut ids = std::collections::HashSet::new();
            anyhow::ensure!(
                response
                    .servers
                    .iter()
                    .all(|server| !server.id.is_empty() && ids.insert(server.id.clone())),
                "mcp_catalog_identity_invalid"
            );
            Ok(response)
        });
        let succeeded = response.is_ok();
        catalog.request = None;
        catalog.poll.complete(
            request.generation,
            succeeded,
            self.timeline_started.elapsed(),
        );
        let mut next = (*catalog.publication).clone();
        match response {
            Ok(response) => {
                if response.snapshot_version < catalog.version {
                    for (_, sender) in std::mem::take(&mut catalog.waiters) {
                        let _ = sender.try_send(catalog.publication.clone());
                    }
                    return;
                }
                catalog.version = response.snapshot_version;
                let mut ids = std::collections::HashSet::new();
                if response
                    .servers
                    .iter()
                    .any(|server| server.id.is_empty() || !ids.insert(server.id.clone()))
                {
                    next.request = McpLoadState::Failed;
                } else {
                    next.servers = response
                        .servers
                        .into_iter()
                        .map(|mut server| {
                            if self
                                .mcp_action_snapshot(&request.workspace, &server.id)
                                .is_some_and(|p| {
                                    p.state == super::operations::McpActionState::Pending
                                        && p.kind == super::operations::McpActionKind::Policy
                                })
                                && let Some(old) = catalog
                                    .publication
                                    .servers
                                    .iter()
                                    .find(|old| old.id == server.id)
                            {
                                server.policy = old.policy.clone();
                                if !server.policy.enabled {
                                    server.status = pioneer_protocol::McpServerStatus::Disabled;
                                }
                            }
                            catalog
                                .publication
                                .servers
                                .iter()
                                .find(|old| ***old == server)
                                .cloned()
                                .unwrap_or_else(|| Arc::new(server))
                        })
                        .collect();
                    next.request = McpLoadState::Ready;
                }
            }
            Err(_) => next.request = McpLoadState::Failed,
        }
        self.publish_mcp_catalog(catalog, next);
        for (_, sender) in std::mem::take(&mut catalog.waiters) {
            let _ = sender.try_send(catalog.publication.clone());
        }
        let route = self.navigation_snapshot();
        let removed = route.workspace_id() == Some(request.workspace.as_str())
            && route.mcp_server_id().is_some_and(|id| {
                catalog.publication.request == McpLoadState::Ready
                    && !catalog.publication.servers.iter().any(|s| s.id == id)
            });
        if succeeded {
            for (id, detail) in &mut catalog.details {
                if detail.request.is_none()
                    && detail.publication.request != McpLoadState::Failed
                    && catalog
                        .consumers
                        .values()
                        .any(|server| server.as_ref() == Some(id))
                {
                    detail.queued = true;
                }
            }
            owner.wake();
        }
        drop(owner);
        if removed {
            self.navigate_mcp(super::route::McpRoute::List);
        }
    }
    fn complete_mcp_details(
        &self,
        request: ReadRequest,
        response: anyhow::Result<McpServerDetailsResponse>,
    ) {
        if self.provider_runtime_epoch() != request.epoch || !self.mcp_allowed(&request.workspace) {
            return;
        }
        let Some(server) = &request.server else {
            return;
        };
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        let Some(catalog) = owner.catalogs.get_mut(&request.workspace) else {
            return;
        };
        if !catalog
            .consumers
            .values()
            .any(|id| id.as_ref() == Some(server))
        {
            return;
        }
        let Some(detail) = catalog.details.get_mut(server) else {
            return;
        };
        if detail.request.as_ref() != Some(&request) {
            return;
        }
        detail.request = None;
        if response.as_ref().is_ok_and(|response| {
            &response.server.id != server || response.snapshot_version < catalog.version
        }) {
            // Discard an invalid transport completion without changing its projection.
            // The request is retired so explicit retry can acquire a fresh generation.
            detail.waiters.clear();
            return;
        }
        let mut next = (*detail.publication).clone();
        match response {
            Ok(mut response) => {
                catalog.version = catalog.version.max(response.snapshot_version);
                // Transport clock/version metadata is not part of the displayed detail.
                response.snapshot_version = 0;
                response.generated_at = 0;
                if next.details.as_deref() != Some(&response) {
                    next.details = Some(Arc::new(response));
                }
                next.request = McpLoadState::Ready;
            }
            Err(_) => next.request = McpLoadState::Failed,
        }
        self.publish_mcp_details(detail, next);
        for (_, sender) in std::mem::take(&mut detail.waiters) {
            let _ = sender.try_send(detail.publication.clone());
        }
    }
    pub(crate) fn resume_mcp_demand(&self) {
        self.mcp_controller
            .lock()
            .expect("Mcp controller poisoned")
            .fenced = false;
        let epoch = self.provider_runtime_epoch();
        let now = self.timeline_started.elapsed();
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        owner.suspended = false;
        for (workspace, catalog) in &mut owner.catalogs {
            if catalog.epoch != epoch {
                catalog.poll.set_connected(false, now);
                catalog.request = None;
                for detail in catalog.details.values_mut() {
                    detail.request = None;
                    detail.queued = false;
                }
                catalog.epoch = epoch;
            }
            catalog
                .poll
                .set_connected(epoch.2.is_some() && self.mcp_allowed(workspace), now);
        }
        owner.wake();
    }
    fn mcp_wait_duration(&self) -> Option<Duration> {
        let epoch = self.provider_runtime_epoch();
        let owner = self.mcp_store.lock().expect("MCP store poisoned");
        if owner.suspended {
            return None;
        }
        let now = self.timeline_started.elapsed();
        owner
            .catalogs
            .iter()
            .filter(|(workspace, catalog)| {
                catalog.epoch == epoch && epoch.2.is_some() && self.mcp_allowed(workspace)
            })
            .filter_map(|(_, catalog)| {
                if !catalog.consumers.is_empty()
                    && catalog.epoch.2.is_some()
                    && catalog.details.iter().any(|(id, detail)| {
                        detail.queued
                            && catalog
                                .consumers
                                .values()
                                .any(|server| server.as_ref() == Some(id))
                    })
                {
                    Some(Duration::ZERO)
                } else {
                    catalog.poll.due().map(|due| due.saturating_sub(now))
                }
            })
            .min()
    }
    pub(crate) fn start_mcp_controller(self: &Arc<Self>) {
        let (wake, receiver) = mpsc::sync_channel(1);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-mcp-catalog".into())
            .spawn(move || {
                loop {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if core.is_stopped() {
                        return;
                    }
                    let request = core.next_mcp_read();
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    if let Some(request) = request {
                        if let Some(server) = &request.server {
                            let response = sender.mcp_server_details(
                                super::details::mcp_server_details_params(
                                    &request.workspace,
                                    server,
                                ),
                            );
                            if let Some(core) = weak.upgrade() {
                                core.complete_mcp_details(request, response);
                            }
                        } else {
                            let response =
                                sender.mcp_list(super::list::mcp_list_params(&request.workspace));
                            if let Some(core) = weak.upgrade() {
                                core.complete_mcp_catalog(request, response);
                            }
                        }
                    }
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    let wait = core.mcp_wait_duration();
                    drop(core);
                    match wait {
                        Some(wait) => {
                            if matches!(
                                receiver.recv_timeout(wait),
                                Err(mpsc::RecvTimeoutError::Disconnected)
                            ) {
                                return;
                            }
                        }
                        None => {
                            if receiver.recv().is_err() {
                                return;
                            }
                        }
                    }
                }
            })
            .expect("MCP catalog worker");
        let mut owner = self.mcp_store.lock().expect("MCP store poisoned");
        owner.wake = Some(wake);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::*;

    fn fixture() -> Arc<ClientCore> {
        let core = Arc::new(ClientCore::new());
        core.accept_authorization_projection(
            0,
            None,
            AuthorizationCapabilitySnapshot {
                schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                authorization_revision: 1,
                principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                role_key: "admin".into(),
                role: AuthorizationRolePresentation {
                    key: "admin".into(),
                    display_name: "Administrator".into(),
                    description: String::new(),
                    built_in: false,
                },
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
    fn response(names: &[&str]) -> McpListResponse {
        McpListResponse {
            snapshot_version: 1,
            generated_at: 1,
            servers: names
                .iter()
                .map(|id| McpListItem {
                    id: (*id).into(),
                    name: (*id).into(),
                    display_name: None,
                    scope: McpScopeKind::Workspace,
                    policy: McpPolicyState {
                        enabled: true,
                        allow_implicit_invocation: false,
                    },
                    required: false,
                    runtime: McpRuntimeStatus {
                        state: McpRuntimeState::Ready,
                        live: true,
                        last_seen_at: None,
                    },
                    tools_count: 1,
                    resources_count: 0,
                    resource_templates_count: 0,
                    prompts_count: 0,
                    status: McpServerStatus::Ready,
                })
                .collect(),
        }
    }
    fn claim(core: &ClientCore, workspace: &str) -> ReadRequest {
        let epoch = core.provider_runtime_epoch();
        let mut owner = core.mcp_store.lock().unwrap();
        let catalog = owner.catalogs.get_mut(workspace).unwrap();
        catalog.poll.set_connected(true, Duration::ZERO);
        catalog.poll.refresh(Duration::ZERO);
        let generation = catalog.poll.claim(Duration::ZERO).unwrap();
        let request = ReadRequest {
            workspace: workspace.into(),
            server: None,
            generation,
            epoch,
        };
        catalog.request = Some(request.clone());
        request
    }
    #[test]
    fn equal_catalog_and_duplicate_completion_preserve_publication_and_rows() {
        let core = fixture();
        let _lease = core.acquire_mcp_demand("workspace", None);
        let first = claim(&core, "workspace");
        core.complete_mcp_catalog(first.clone(), Ok(response(&["A", "B"])));
        let publication = core.mcp_catalog_snapshot("workspace").unwrap();
        core.complete_mcp_catalog(first, Ok(response(&["C"])));
        assert!(Arc::ptr_eq(
            &publication,
            &core.mcp_catalog_snapshot("workspace").unwrap()
        ));
        let next = claim(&core, "workspace");
        core.complete_mcp_catalog(next, Ok(response(&["A", "B"])));
        assert!(Arc::ptr_eq(
            &publication,
            &core.mcp_catalog_snapshot("workspace").unwrap()
        ));
        let next = claim(&core, "workspace");
        let mut changed = response(&["B", "A"]);
        changed.servers[1].status = McpServerStatus::Disabled;
        core.complete_mcp_catalog(next, Ok(changed));
        let updated = core.mcp_catalog_snapshot("workspace").unwrap();
        assert_eq!(updated.revision(), publication.revision() + 1);
        assert!(Arc::ptr_eq(
            &publication.servers()[1],
            &updated.servers()[0]
        ));
        assert!(!Arc::ptr_eq(
            &publication.servers()[0],
            &updated.servers()[1]
        ));
        assert!(
            core.snapshot(&ClientScope::Skills {
                workspace_id: Some("workspace".into())
            })
            .is_none()
        );
        assert!(
            core.snapshot(&ClientScope::McpDetails {
                workspace_id: "workspace".into(),
                server_id: "B".into()
            })
            .is_none()
        );
    }
    #[test]
    fn wrong_scope_release_and_invalid_catalog_do_not_accept_late_content() {
        let core = fixture();
        let lease = core.acquire_mcp_demand("workspace", None);
        let request = claim(&core, "workspace");
        let mut wrong = request.clone();
        wrong.workspace = "other".into();
        core.complete_mcp_catalog(wrong, Ok(response(&["A"])));
        assert!(core.mcp_catalog_snapshot("workspace").is_none());
        core.complete_mcp_catalog(request, Ok(response(&["A", "A"])));
        assert_eq!(
            core.mcp_catalog_snapshot("workspace").unwrap().request(),
            McpLoadState::Failed
        );
        assert_eq!(
            core.mcp_store.lock().unwrap().catalogs["workspace"]
                .poll
                .due(),
            None
        );
        let request = claim(&core, "workspace");
        drop(lease);
        core.complete_mcp_catalog(request, Ok(response(&["A"])));
        assert!(
            core.mcp_catalog_snapshot("workspace")
                .unwrap()
                .servers()
                .is_empty()
        );
    }
    #[test]
    fn lease_does_not_retain_client_owner() {
        let core = fixture();
        let weak = Arc::downgrade(&core);
        let lease = core.acquire_mcp_demand("workspace", None);
        drop(core);
        assert!(weak.upgrade().is_none());
        drop(lease);
    }
    #[test]
    fn releasing_the_last_detail_consumer_cancels_it_while_list_remains_active() {
        let core = fixture();
        let _list = core.acquire_mcp_demand("workspace", None);
        let detail = core.acquire_mcp_demand("workspace", Some("A"));
        {
            let mut owner = core.mcp_store.lock().unwrap();
            let detail = owner
                .catalogs
                .get_mut("workspace")
                .unwrap()
                .details
                .get_mut("A")
                .unwrap();
            detail.queued = false;
            detail.request = Some(ReadRequest {
                workspace: "workspace".into(),
                server: Some("A".into()),
                generation: 1,
                epoch: core.provider_runtime_epoch(),
            });
        }
        drop(detail);
        let owner = core.mcp_store.lock().unwrap();
        let catalog = &owner.catalogs["workspace"];
        let consumer_count = catalog.consumers.len();
        let cancelled = catalog.details["A"].request.is_none() && !catalog.details["A"].queued;
        drop(owner);
        assert_eq!(consumer_count, 1);
        assert!(cancelled);
    }

    #[test]
    fn forbidden_detail_demand_has_no_ready_deadline() {
        let core = Arc::new(ClientCore::new());
        let _detail = core.acquire_mcp_demand("workspace", Some("A"));
        core.mcp_store
            .lock()
            .unwrap()
            .catalogs
            .get_mut("workspace")
            .unwrap()
            .epoch = (0, 0, Some(7));
        assert_eq!(core.mcp_wait_duration(), None);
    }
    #[test]
    fn detail_transport_metadata_status_and_wrong_target_preserve_exact_scopes() {
        use pioneer_protocol::*;
        let core = crate::catalog_test_support::client();
        core.accept_mcp_catalog_for_test("workspace", response(&["a", "b"]));
        let a = core.acquire_mcp_demand("workspace", Some("a"));
        let make_detail = || McpServerDetailsResponse {
            snapshot_version: 1,
            generated_at: 1,
            server: response(&["a"]).servers.remove(0),
            catalog: McpServerCatalogDetails {
                catalog_version: None,
                generated_at: None,
                server_info: serde_json::Value::Null,
                server_instructions_hash: None,
                tools: vec![],
                resources: vec![],
                resource_templates: vec![],
                prompts: vec![],
            },
            management: None,
        };
        let request = core.next_mcp_read().unwrap();
        assert_eq!(request.server.as_deref(), Some("a"));
        core.complete_mcp_details(request.clone(), Ok(make_detail()));
        let first = core.mcp_details_snapshot("workspace", "a").unwrap();
        core.complete_mcp_details(request, Ok(make_detail()));
        assert!(Arc::ptr_eq(
            &first,
            &core.mcp_details_snapshot("workspace", "a").unwrap()
        ));
        core.refresh_mcp("workspace");
        let request = core.next_mcp_read().unwrap();
        let mut equal = make_detail();
        equal.snapshot_version = 2;
        equal.generated_at = 99;
        core.complete_mcp_details(request, Ok(equal));
        assert!(Arc::ptr_eq(
            &first,
            &core.mcp_details_snapshot("workspace", "a").unwrap()
        ));
        core.refresh_mcp("workspace");
        let old_request = core.next_mcp_read().unwrap();
        let mut old_detail = make_detail();
        old_detail.server.display_name = Some("stale display".into());
        core.complete_mcp_details(old_request, Ok(old_detail));
        assert!(Arc::ptr_eq(
            &first,
            &core.mcp_details_snapshot("workspace", "a").unwrap()
        ));
        let list = core.mcp_catalog_snapshot("workspace").unwrap();
        let b = list.servers()[1].clone();
        let status =
            GatewayNotification::McpServerStatusChanged(McpServerStatusChangedNotification {
                workspace_id: "workspace".into(),
                snapshot_version: 3,
                server: McpServerStatusItem {
                    id: "a".into(),
                    name: "a".into(),
                    scope_kind: McpScopeKind::Workspace,
                    runtime: make_detail().server.runtime,
                    status: McpServerStatus::Restarting,
                },
            });
        core.observe_mcp_notification(&status);
        let changed = core.mcp_details_snapshot("workspace", "a").unwrap();
        let changed_list = core.mcp_catalog_snapshot("workspace").unwrap();
        assert_eq!(changed.revision(), first.revision() + 1);
        assert!(Arc::ptr_eq(&b, &changed_list.servers()[1]));
        core.observe_mcp_notification(&status);
        assert!(Arc::ptr_eq(
            &changed,
            &core.mcp_details_snapshot("workspace", "a").unwrap()
        ));
        assert!(Arc::ptr_eq(
            &changed_list,
            &core.mcp_catalog_snapshot("workspace").unwrap()
        ));
        core.refresh_mcp("workspace");
        let request = core.next_mcp_read().unwrap();
        let mut wrong = make_detail();
        wrong.server.id = "b".into();
        core.complete_mcp_details(request, Ok(wrong));
        assert!(Arc::ptr_eq(
            &changed,
            &core.mcp_details_snapshot("workspace", "a").unwrap()
        ));
        core.refresh_mcp("workspace");
        let request = core.next_mcp_read().unwrap();
        core.complete_mcp_details(request, Ok(make_detail()));
        assert!(Arc::ptr_eq(
            &changed,
            &core.mcp_details_snapshot("workspace", "a").unwrap()
        ));
        core.refresh_mcp("workspace");
        let request = core.next_mcp_read().unwrap();
        drop(a);
        let after = core.mcp_details_snapshot("workspace", "a").unwrap();
        core.complete_mcp_details(request, Ok(make_detail()));
        assert!(Arc::ptr_eq(
            &after,
            &core.mcp_details_snapshot("workspace", "a").unwrap()
        ));
    }

    #[test]
    fn removed_destination_returns_to_its_list_without_rewriting_a_sibling_route() {
        use crate::navigation::{NavigationIntent, SemanticDestination};
        let core = crate::catalog_test_support::client();
        core.navigate(
            NavigationIntent::SelectWorkspace {
                workspace_id: Some("workspace".into()),
            },
            None,
        );
        core.accept_mcp_catalog_for_test("workspace", response(&["a", "b"]));
        core.navigate_mcp(super::super::route::McpRoute::Details("a".into()));
        core.accept_mcp_catalog_for_test("workspace", response(&["b"]));
        assert!(matches!(
            core.navigation_snapshot().destination(),
            crate::navigation::SemanticDestination::Mcp { server_id: None }
        ));
        core.navigate(
            NavigationIntent::Navigate {
                destination: SemanticDestination::Skills { skill_id: None },
            },
            None,
        );
        core.accept_mcp_catalog_for_test("workspace", response(&[]));
        assert!(matches!(
            core.navigation_snapshot().destination(),
            SemanticDestination::Skills { .. }
        ));
    }
    #[test]
    fn action_success_fences_a_read_started_before_the_mutation() {
        let core = crate::catalog_test_support::client();
        let _lease = core.acquire_mcp_demand("workspace", None);
        let request = core.next_mcp_read().unwrap();
        let before = core.mcp_catalog_snapshot("workspace").unwrap();
        core.fence_mcp_reads_after_action("workspace", "a");
        core.complete_mcp_catalog(request, Ok(response(&["a"])));
        assert!(Arc::ptr_eq(
            &before,
            &core.mcp_catalog_snapshot("workspace").unwrap()
        ));
        assert!(core.next_mcp_read().is_some());
    }
}
