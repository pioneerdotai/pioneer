//! Process-local Skills catalog, detail and recurring read ownership.
use super::catalog::{SkillManagementProjection, SkillsCatalogSnapshot};
use crate::{core::*, request_state::poll::PollRequest};
use pioneer_protocol::{SkillHealthItem, SkillId, SkillListItem};
use std::{
    collections::BTreeMap,
    sync::{Arc, Weak, mpsc},
    time::Duration,
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillsLoadState {
    #[default]
    Idle,
    Loading,
    Ready,
    Failed,
    Cancelled,
    Forbidden,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SkillsCatalogPublication {
    pub workspace_id: String,
    pub revision: u64,
    pub catalog: Vec<Arc<SkillListItem>>,
    pub installed: Vec<Arc<SkillListItem>>,
    pub management: Arc<SkillManagementProjection>,
    pub request: SkillsLoadState,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SkillsDetailsPublication {
    pub workspace_id: String,
    pub skill_id: SkillId,
    pub revision: u64,
    pub skill: Option<Arc<SkillListItem>>,
    pub health: Option<Arc<SkillHealthItem>>,
    pub request: SkillsLoadState,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadRequest {
    workspace: String,
    generation: u64,
    epoch: (u64, u64, Option<u64>),
    management: bool,
}
struct Catalog {
    publication: Arc<SkillsCatalogPublication>,
    health: BTreeMap<SkillId, Arc<SkillHealthItem>>,
    details: BTreeMap<SkillId, Arc<SkillsDetailsPublication>>,
    consumers: BTreeMap<u64, Option<SkillId>>,
    management_consumers: std::collections::BTreeSet<u64>,
    waiters: BTreeMap<u64, mpsc::SyncSender<Arc<SkillsCatalogPublication>>>,
    poll: PollRequest,
    request: Option<ReadRequest>,
    epoch: (u64, u64, Option<u64>),
}
#[derive(Default)]
pub(crate) struct SkillsStore {
    binding_gate: Arc<std::sync::Mutex<()>>,
    suspended: bool,
    binding_demands: std::collections::HashMap<ClientScope, u64>,
    catalogs: BTreeMap<String, Catalog>,
    next_consumer: u64,
    wake: Option<mpsc::SyncSender<()>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl SkillsStore {
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
impl Drop for SkillsStore {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
/// Active route/picker demand; a retained warm view holds no lease.
pub struct SkillsDemand {
    core: Weak<ClientCore>,
    workspace: String,
    identity: u64,
}
impl Drop for SkillsDemand {
    fn drop(&mut self) {
        if let Some(core) = self.core.upgrade() {
            core.release_skills_demand(&self.workspace, self.identity);
        }
    }
}
pub struct SkillsRead {
    _demand: SkillsDemand,
    receiver: mpsc::Receiver<Arc<SkillsCatalogPublication>>,
}
impl SkillsRead {
    pub fn wait_while(
        self,
        current: impl Fn() -> bool,
    ) -> anyhow::Result<Arc<SkillsCatalogPublication>> {
        loop {
            anyhow::ensure!(current(), "skills_read_cancelled");
            match self.receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(p) => return Ok(p),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(_) => anyhow::bail!("skills_read_cancelled"),
            }
        }
    }
}
impl ClientCore {
    pub(crate) fn skills_binding_demand_changed(&self, scope: &ClientScope, demand: ClientDemand) {
        let gate = self
            .skills_store
            .lock()
            .expect("catalog store poisoned")
            .binding_gate
            .clone();
        let _binding = gate.lock().expect("catalog binding demand poisoned");
        let demand = self.current_scope_demand(scope).unwrap_or(demand);
        if let ClientScope::SkillsUpload {
            workspace_id,
            operation_id,
        } = scope
        {
            if demand == ClientDemand::Suspended {
                self.cancel_skill_upload(workspace_id, *operation_id);
            }
            return;
        }
        let (workspace, skill) = match scope {
            ClientScope::Skills {
                workspace_id: Some(w),
            } => (w.as_str(), None),
            ClientScope::SkillsDetails {
                workspace_id,
                skill_id,
            } => (workspace_id.as_str(), Some(skill_id)),
            _ => return,
        };
        if demand == ClientDemand::Suspended {
            let identity = self
                .skills_store
                .lock()
                .expect("Skills store poisoned")
                .binding_demands
                .remove(scope);
            if let Some(identity) = identity {
                self.release_skills_demand(workspace, identity);
            }
            return;
        }
        if self
            .skills_store
            .lock()
            .expect("Skills store poisoned")
            .binding_demands
            .contains_key(scope)
        {
            return;
        }
        let lease = self.acquire_skills_demand_kind(workspace, skill, true, Weak::new());
        self.skills_store
            .lock()
            .expect("Skills store poisoned")
            .binding_demands
            .insert(scope.clone(), lease.identity);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn skills_demand_count_for_test(&self, workspace: &str) -> usize {
        self.skills_store
            .lock()
            .unwrap()
            .catalogs
            .get(workspace)
            .map_or(0, |c| c.consumers.len())
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn accept_skills_catalog_for_test(
        self: &Arc<Self>,
        workspace: &str,
        response: SkillsCatalogSnapshot,
    ) {
        let _demand = self.acquire_skills_demand(workspace, None);
        self.refresh_skills(workspace);
        let request = self.next_skills_read().expect("synthetic catalog demand");
        self.complete_skills_catalog(request, Ok(response));
    }

    pub fn read_skills_catalog(self: &Arc<Self>, workspace: &str) -> SkillsRead {
        let demand = self.acquire_skills_demand_kind(workspace, None, false, Arc::downgrade(self));
        let (sender, receiver) = mpsc::sync_channel(1);
        if self.provider_runtime_epoch().2.is_none() || !self.skills_allowed(workspace) {
            drop(sender);
            return SkillsRead {
                _demand: demand,
                receiver,
            };
        }
        let mut owner = self.skills_store.lock().expect("Skills store poisoned");
        let catalog = owner.catalogs.get_mut(workspace).unwrap();
        catalog.waiters.insert(demand.identity, sender);
        catalog.poll.refresh(self.timeline_started.elapsed());
        owner.wake();
        SkillsRead {
            _demand: demand,
            receiver,
        }
    }

    pub fn skills_catalog_snapshot(
        &self,
        workspace: &str,
    ) -> Option<Arc<SkillsCatalogPublication>> {
        self.snapshot(&ClientScope::Skills {
            workspace_id: Some(workspace.into()),
        })
        .and_then(|p| p.snapshot().payload())
    }
    pub fn skills_details_snapshot(
        &self,
        workspace: &str,
        skill: &SkillId,
    ) -> Option<Arc<SkillsDetailsPublication>> {
        self.snapshot(&ClientScope::SkillsDetails {
            workspace_id: workspace.into(),
            skill_id: skill.clone(),
        })
        .and_then(|p| p.snapshot().payload())
    }
    pub(crate) fn skills_allowed(&self, workspace: &str) -> bool {
        !self.is_stopped()
            && self
                .authorization_snapshot(Some(workspace), None)
                .or_else(|| self.authorization_snapshot(None, None))
                .is_some_and(|p| {
                    let c = crate::authorization::principal_presentation_capabilities(&p);
                    c.can_use_skills || c.can_manage_capabilities
                })
    }
    pub fn acquire_skills_demand(
        self: &Arc<Self>,
        workspace: &str,
        skill: Option<&SkillId>,
    ) -> SkillsDemand {
        self.acquire_skills_demand_kind(workspace, skill, true, Arc::downgrade(self))
    }
    fn acquire_skills_demand_kind(
        &self,
        workspace: &str,
        skill: Option<&SkillId>,
        management: bool,
        core: Weak<ClientCore>,
    ) -> SkillsDemand {
        let epoch = self.provider_runtime_epoch();
        let now = self.timeline_started.elapsed();
        let mut owner = self.skills_store.lock().expect("Skills store poisoned");
        owner.next_consumer = owner
            .next_consumer
            .checked_add(1)
            .expect("Skills demand exhausted");
        let identity = owner.next_consumer;
        let catalog = owner
            .catalogs
            .entry(workspace.into())
            .or_insert_with(|| Catalog {
                publication: Arc::new(SkillsCatalogPublication {
                    workspace_id: workspace.into(),
                    revision: 0,
                    catalog: vec![],
                    installed: vec![],
                    management: Arc::default(),
                    request: SkillsLoadState::Idle,
                }),
                health: BTreeMap::new(),
                details: BTreeMap::new(),
                consumers: BTreeMap::new(),
                management_consumers: Default::default(),
                waiters: BTreeMap::new(),
                poll: PollRequest::new(Duration::from_secs(20)),
                request: None,
                epoch,
            });
        catalog
            .poll
            .set_connected(epoch.2.is_some() && self.skills_allowed(workspace), now);
        catalog.poll.acquire(identity, now);
        catalog.consumers.insert(identity, skill.cloned());
        if management {
            catalog.management_consumers.insert(identity);
        }
        if let Some(skill) = skill {
            catalog.details.entry(skill.clone()).or_insert_with(|| {
                Arc::new(SkillsDetailsPublication {
                    workspace_id: workspace.into(),
                    skill_id: skill.clone(),
                    revision: 0,
                    skill: None,
                    health: None,
                    request: SkillsLoadState::Idle,
                })
            });
            self.project_skills_details(catalog);
        }
        owner.wake();
        SkillsDemand {
            core,
            workspace: workspace.into(),
            identity,
        }
    }
    fn release_skills_demand(&self, workspace: &str, identity: u64) {
        let mut owner = self
            .skills_store
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(catalog) = owner.catalogs.get_mut(workspace) {
            catalog.consumers.remove(&identity);
            catalog.management_consumers.remove(&identity);
            catalog.waiters.remove(&identity);
            catalog.poll.release(identity);
            if catalog.consumers.is_empty() && catalog.request.take().is_some() {
                let mut next = (*catalog.publication).clone();
                if next.request == SkillsLoadState::Loading {
                    next.request = SkillsLoadState::Cancelled;
                }
                self.publish_skills_catalog(catalog, next);
                self.project_skills_details(catalog);
            }
        }
        owner.wake();
    }
    pub(crate) fn apply_skills_policy(
        &self,
        workspace: &str,
        id: &SkillId,
        enabled: bool,
        implicit: bool,
    ) {
        let mut owner = self.skills_store.lock().expect("Skills store poisoned");
        let Some(catalog) = owner.catalogs.get_mut(workspace) else {
            return;
        };
        let mut next = (*catalog.publication).clone();
        for items in [&mut next.catalog, &mut next.installed] {
            for row in items {
                if &row.skill_id == id {
                    let mut value = (**row).clone();
                    super::actions::apply_local_skill_policy(
                        std::slice::from_mut(&mut value),
                        &mut [],
                        id,
                        enabled,
                        implicit,
                    );
                    if **row != value {
                        *row = Arc::new(value);
                    }
                }
            }
        }
        let installed = next
            .installed
            .iter()
            .map(|s| (**s).clone())
            .collect::<Vec<_>>();
        let management = super::catalog::project_skill_management(
            &installed,
            next.management
                .packs
                .iter()
                .map(|p| p.pack.clone())
                .collect(),
        );
        if *next.management != management {
            next.management = Arc::new(management);
        }
        self.publish_skills_catalog(catalog, next);
        self.project_skills_details(catalog);
    }
    pub(crate) fn fence_skills_reads_after_action(&self, workspace: &str) {
        let now = self.timeline_started.elapsed();
        let connected = self.provider_runtime_epoch().2.is_some() && self.skills_allowed(workspace);
        let mut owner = self.skills_store.lock().expect("catalog store poisoned");
        if let Some(catalog) = owner.catalogs.get_mut(workspace) {
            catalog.request = None;
            catalog.poll.set_connected(false, now);
            catalog.poll.set_connected(connected, now);
        }
        owner.wake();
    }
    pub fn refresh_skills(&self, workspace: &str) {
        let mut owner = self.skills_store.lock().expect("Skills store poisoned");
        if let Some(catalog) = owner.catalogs.get_mut(workspace) {
            catalog.poll.refresh(self.timeline_started.elapsed());
        }
        owner.wake();
    }
    fn publish_skills_catalog(&self, catalog: &mut Catalog, mut next: SkillsCatalogPublication) {
        next.revision = catalog.publication.revision;
        if *catalog.publication == next {
            return;
        }
        let scope = ClientScope::Skills {
            workspace_id: Some(next.workspace_id.clone()),
        };
        next.revision = next
            .revision
            .max(
                self.snapshot(&scope)
                    .map_or(0, |p| p.revisions().scoped().get()),
            )
            .checked_add(1)
            .expect("Skills revision exhausted");
        catalog.publication = Arc::new(next);
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(catalog.publication.revision),
            catalog.publication.clone(),
            vec![],
        );
    }
    fn project_skills_details(&self, catalog: &mut Catalog) {
        for (id, publication) in &mut catalog.details {
            let mut next = (**publication).clone();
            next.skill = catalog
                .publication
                .installed
                .iter()
                .chain(&catalog.publication.catalog)
                .find(|s| &s.skill_id == id)
                .cloned();
            next.health = catalog.health.get(id).cloned();
            next.request = catalog.publication.request;
            if **publication == next {
                continue;
            }
            let scope = ClientScope::SkillsDetails {
                workspace_id: next.workspace_id.clone(),
                skill_id: id.clone(),
            };
            next.revision = next
                .revision
                .max(
                    self.snapshot(&scope)
                        .map_or(0, |p| p.revisions().scoped().get()),
                )
                .checked_add(1)
                .expect("Skill detail revision exhausted");
            *publication = Arc::new(next);
            self.publish(
                &ClientMutationAuthority { _private: () },
                scope,
                crate::threads::registry::revisions(publication.revision),
                publication.clone(),
                vec![],
            );
        }
    }
    pub(crate) fn invalidate_skills(&self) {
        let mut owner = self.skills_store.lock().expect("Skills store poisoned");
        owner.suspended = true;
        for catalog in owner.catalogs.values_mut() {
            catalog
                .poll
                .set_connected(false, self.timeline_started.elapsed());
            catalog.request = None;
            catalog.waiters.clear();
            catalog.health.clear();
            let mut next = (*catalog.publication).clone();
            next.catalog.clear();
            next.installed.clear();
            next.management = Arc::default();
            next.request = SkillsLoadState::Cancelled;
            self.publish_skills_catalog(catalog, next);
            self.project_skills_details(catalog);
        }
        owner.wake();
    }
    pub(crate) fn resume_skills_demand(&self) {
        self.skills_controller
            .lock()
            .expect("Skills controller poisoned")
            .fenced = false;
        let epoch = self.provider_runtime_epoch();
        let now = self.timeline_started.elapsed();
        let mut owner = self.skills_store.lock().expect("Skills store poisoned");
        owner.suspended = false;
        for (workspace, catalog) in &mut owner.catalogs {
            if catalog.epoch != epoch {
                catalog.poll.set_connected(false, now);
                catalog.request = None;
                catalog.epoch = epoch;
            }
            catalog
                .poll
                .set_connected(epoch.2.is_some() && self.skills_allowed(workspace), now);
        }
        owner.wake();
    }
    fn next_skills_read(&self) -> Option<ReadRequest> {
        let epoch = self.provider_runtime_epoch();
        let now = self.timeline_started.elapsed();
        let mut owner = self.skills_store.lock().expect("Skills store poisoned");
        if owner.suspended {
            return None;
        }
        for (workspace, catalog) in &mut owner.catalogs {
            if catalog.epoch != epoch {
                catalog.poll.set_connected(false, now);
                catalog.request = None;
                catalog.epoch = epoch;
            }
            catalog
                .poll
                .set_connected(epoch.2.is_some() && self.skills_allowed(workspace), now);
            let Some(generation) = catalog.poll.claim(now) else {
                continue;
            };
            let management = !catalog.management_consumers.is_empty()
                && self
                    .authorization_snapshot(Some(workspace), None)
                    .or_else(|| self.authorization_snapshot(None, None))
                    .is_some_and(|p| {
                        crate::authorization::principal_presentation_capabilities(&p)
                            .can_manage_capabilities
                    });
            let request = ReadRequest {
                workspace: workspace.clone(),
                generation,
                epoch,
                management,
            };
            catalog.request = Some(request.clone());
            if catalog.publication.catalog.is_empty()
                && catalog.publication.request != SkillsLoadState::Ready
            {
                let mut next = (*catalog.publication).clone();
                next.request = SkillsLoadState::Loading;
                self.publish_skills_catalog(catalog, next);
                self.project_skills_details(catalog);
            }
            return Some(request);
        }
        None
    }
    fn complete_skills_catalog(
        &self,
        request: ReadRequest,
        response: anyhow::Result<SkillsCatalogSnapshot>,
    ) {
        if self.provider_runtime_epoch() != request.epoch
            || !self.skills_allowed(&request.workspace)
        {
            return;
        }
        let mut owner = self.skills_store.lock().expect("Skills store poisoned");
        let Some(catalog) = owner.catalogs.get_mut(&request.workspace) else {
            return;
        };
        if catalog.request.as_ref() != Some(&request)
            || !catalog.poll.is_current(request.generation)
        {
            return;
        }
        let response = response.and_then(|response| {
            for items in [&response.catalog, &response.installed] {
                let mut ids = std::collections::HashSet::new();
                anyhow::ensure!(
                    items.iter().all(|item| ids.insert(&item.skill_id)),
                    "skill_catalog_identity_invalid"
                );
            }
            Ok(response)
        });
        catalog.request = None;
        catalog.poll.complete(
            request.generation,
            response.is_ok(),
            self.timeline_started.elapsed(),
        );
        let mut next = (*catalog.publication).clone();
        match response {
            Ok(mut response) => {
                for item in response.catalog.iter_mut().chain(&mut response.installed) {
                    if self
                        .skills_action_snapshot(&request.workspace, &item.skill_id.to_string())
                        .is_some_and(|p| {
                            p.state == super::operations::SkillsActionState::Pending
                                && p.kind == super::operations::SkillsActionKind::Policy
                        })
                        && let Some(old) = catalog
                            .publication
                            .catalog
                            .iter()
                            .find(|s| s.skill_id == item.skill_id)
                    {
                        let id = item.skill_id.clone();
                        super::actions::apply_local_skill_policy(
                            std::slice::from_mut(item),
                            &mut [],
                            &id,
                            old.policy.enabled,
                            old.policy.allow_implicit_invocation,
                        );
                    }
                }
                response.management = super::catalog::project_skill_management(
                    &response.installed,
                    response
                        .management
                        .packs
                        .iter()
                        .map(|p| p.pack.clone())
                        .collect(),
                );
                let reuse = |items: Vec<SkillListItem>| {
                    items
                        .into_iter()
                        .map(|item| {
                            catalog
                                .publication
                                .catalog
                                .iter()
                                .chain(&catalog.publication.installed)
                                .find(|old| ***old == item)
                                .cloned()
                                .unwrap_or_else(|| Arc::new(item))
                        })
                        .collect()
                };
                next.catalog = reuse(response.catalog);
                next.installed = reuse(response.installed);
                if *next.management != response.management {
                    next.management = Arc::new(response.management);
                }
                if request.management {
                    catalog.health = response
                        .health_details
                        .into_iter()
                        .map(|(id, item)| {
                            let value = catalog
                                .health
                                .get(&id)
                                .filter(|old| ***old == item)
                                .cloned()
                                .unwrap_or_else(|| Arc::new(item));
                            (id, value)
                        })
                        .collect();
                } else {
                    catalog
                        .health
                        .retain(|id, _| next.installed.iter().any(|s| &s.skill_id == id));
                }
                next.request = SkillsLoadState::Ready;
            }
            Err(_) => next.request = SkillsLoadState::Failed,
        }
        self.publish_skills_catalog(catalog, next);
        self.project_skills_details(catalog);
        for (_, sender) in std::mem::take(&mut catalog.waiters) {
            let _ = sender.try_send(catalog.publication.clone());
        }
        if !request.management
            && !catalog.management_consumers.is_empty()
            && catalog.publication.request == SkillsLoadState::Ready
        {
            catalog.poll.refresh(self.timeline_started.elapsed());
        }
        let route = self.navigation_snapshot();
        let removed = route.workspace_id() == Some(request.workspace.as_str())
            && route.skill_id().is_some_and(|id| {
                catalog.publication.request == SkillsLoadState::Ready
                    && !catalog
                        .publication
                        .installed
                        .iter()
                        .any(|s| &s.skill_id == id)
            });
        drop(owner);
        if removed {
            self.navigate_skills(super::route::SkillsRoute::List);
        }
    }
    fn skills_wait_duration(&self) -> Option<Duration> {
        let owner = self.skills_store.lock().expect("Skills store poisoned");
        let now = self.timeline_started.elapsed();
        if owner.suspended {
            return None;
        }
        owner
            .catalogs
            .iter()
            .filter(|(workspace, _)| self.skills_allowed(workspace))
            .filter_map(|(_, catalog)| catalog.poll.due().map(|due| due.saturating_sub(now)))
            .min()
    }
    pub(crate) fn start_skills_controller(self: &Arc<Self>) {
        let (wake, receiver) = mpsc::sync_channel(1);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-skills-catalog".into())
            .spawn(move || {
                loop {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    if core.is_stopped() {
                        return;
                    }
                    let request = core.next_skills_read();
                    let sender = core.compatibility_runtime().ws_command_sender();
                    drop(core);
                    if let Some(request) = request {
                        let response = super::catalog::load_skills_snapshot(
                            &sender,
                            &request.workspace,
                            request.management,
                        );
                        if let Some(core) = weak.upgrade() {
                            core.complete_skills_catalog(request, response);
                        }
                        continue;
                    }
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    let wait = core.skills_wait_duration();
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
            .expect("Skills controller could not start");
        let mut owner = self.skills_store.lock().expect("Skills store poisoned");
        owner.wake = Some(wake);
        owner.task = Some(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_test_support::{client, skill};
    fn response() -> SkillsCatalogSnapshot {
        super::super::catalog::project_skills_snapshot(vec![skill('A'), skill('B')], vec![])
    }
    #[test]
    fn equality_scoped_details_and_unchanged_rows_survive_reorder() {
        let core = client();
        let _list = core.acquire_skills_demand("workspace", None);
        let a = skill('A').skill_id;
        let b = skill('B').skill_id;
        let _a = core.acquire_skills_demand("workspace", Some(&a));
        let _b = core.acquire_skills_demand("workspace", Some(&b));
        let request = core.next_skills_read().unwrap();
        core.complete_skills_catalog(request.clone(), Ok(response()));
        let first = core.skills_catalog_snapshot("workspace").unwrap();
        let detail_b = core.skills_details_snapshot("workspace", &b).unwrap();
        core.complete_skills_catalog(request, Ok(response()));
        assert!(Arc::ptr_eq(
            &first,
            &core.skills_catalog_snapshot("workspace").unwrap()
        ));
        core.refresh_skills("workspace");
        core.complete_skills_catalog(core.next_skills_read().unwrap(), Ok(response()));
        assert!(Arc::ptr_eq(
            &first,
            &core.skills_catalog_snapshot("workspace").unwrap()
        ));
        let mut changed = response();
        changed.catalog[0].description = "changed A".into();
        changed.installed[0] = changed.catalog[0].clone();
        changed.catalog.reverse();
        changed.installed.reverse();
        core.refresh_skills("workspace");
        core.complete_skills_catalog(core.next_skills_read().unwrap(), Ok(changed));
        let next = core.skills_catalog_snapshot("workspace").unwrap();
        assert_eq!(next.revision, first.revision + 1);
        assert!(Arc::ptr_eq(&first.catalog[1], &next.catalog[0]));
        assert!(Arc::ptr_eq(
            &detail_b,
            &core.skills_details_snapshot("workspace", &b).unwrap()
        ));
        assert!(core.mcp_catalog_snapshot("workspace").is_none());
    }
    #[test]
    fn demand_coalesces_failure_is_explicit_and_last_release_fences_completion() {
        let core = client();
        let first = core.acquire_skills_demand("workspace", None);
        let second = core.acquire_skills_demand("workspace", None);
        let request = core.next_skills_read().unwrap();
        assert!(core.next_skills_read().is_none());
        core.complete_skills_catalog(request, Err(anyhow::anyhow!("synthetic failure")));
        assert_eq!(
            core.skills_catalog_snapshot("workspace").unwrap().request,
            SkillsLoadState::Failed
        );
        assert!(core.next_skills_read().is_none());
        assert!(core.skills_wait_duration().is_none());
        core.refresh_skills("workspace");
        let request = core.next_skills_read().unwrap();
        drop(first);
        assert!(
            core.skills_store.lock().unwrap().catalogs["workspace"]
                .request
                .is_some()
        );
        drop(second);
        let snapshot = core.skills_catalog_snapshot("workspace").unwrap();
        core.complete_skills_catalog(request, Ok(response()));
        assert!(Arc::ptr_eq(
            &snapshot,
            &core.skills_catalog_snapshot("workspace").unwrap()
        ));
        assert!(core.skills_wait_duration().is_none());
        let _again = core.acquire_skills_demand("workspace", None);
        assert!(core.next_skills_read().is_some());
    }
    #[test]
    fn wrong_workspace_duplicate_ids_and_revoked_results_are_ignored() {
        let core = client();
        let _lease = core.acquire_skills_demand("workspace", None);
        let request = core.next_skills_read().unwrap();
        let mut wrong = request.clone();
        wrong.workspace = "other".into();
        let before = core.skills_catalog_snapshot("workspace").unwrap();
        core.complete_skills_catalog(wrong, Ok(response()));
        assert!(Arc::ptr_eq(
            &before,
            &core.skills_catalog_snapshot("workspace").unwrap()
        ));
        let mut invalid = response();
        invalid.catalog.push(invalid.catalog[0].clone());
        core.complete_skills_catalog(request, Ok(invalid));
        assert_eq!(
            core.skills_catalog_snapshot("workspace").unwrap().request,
            SkillsLoadState::Failed
        );
        core.refresh_skills("workspace");
        let request = core.next_skills_read().unwrap();
        core.invalidate_skills();
        let before = core.skills_catalog_snapshot("workspace").unwrap();
        core.complete_skills_catalog(request, Ok(response()));
        assert!(Arc::ptr_eq(
            &before,
            &core.skills_catalog_snapshot("workspace").unwrap()
        ));
    }
    #[test]
    fn lease_and_pending_read_do_not_retain_owner() {
        let core = client();
        let weak = Arc::downgrade(&core);
        let read = core.read_skills_catalog("workspace");
        drop(core);
        assert!(weak.upgrade().is_none());
        assert!(read.wait_while(|| true).is_err());
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
        core.accept_skills_catalog_for_test("workspace", response());
        core.navigate_skills(super::super::route::SkillsRoute::Details(
            skill('A').skill_id,
        ));
        core.accept_skills_catalog_for_test(
            "workspace",
            super::super::catalog::project_skills_snapshot(vec![skill('B')], vec![]),
        );
        assert!(matches!(
            core.navigation_snapshot().destination(),
            SemanticDestination::Skills { skill_id: None }
        ));
        core.navigate(
            NavigationIntent::Navigate {
                destination: SemanticDestination::Mcp { server_id: None },
            },
            None,
        );
        core.accept_skills_catalog_for_test(
            "workspace",
            super::super::catalog::project_skills_snapshot(vec![], vec![]),
        );
        assert!(matches!(
            core.navigation_snapshot().destination(),
            SemanticDestination::Mcp { .. }
        ));
    }
    #[test]
    fn action_success_fences_a_read_started_before_the_mutation() {
        let core = crate::catalog_test_support::client();
        let _lease = core.acquire_skills_demand("workspace", None);
        let request = core.next_skills_read().unwrap();
        let before = core.skills_catalog_snapshot("workspace").unwrap();
        core.fence_skills_reads_after_action("workspace");
        core.complete_skills_catalog(request, Ok(response()));
        assert!(Arc::ptr_eq(
            &before,
            &core.skills_catalog_snapshot("workspace").unwrap()
        ));
        assert!(core.next_skills_read().is_some());
    }
    #[test]
    fn composer_read_does_not_request_management_health_and_active_management_can_upgrade_it() {
        let core = crate::catalog_test_support::client();
        let read = core.read_skills_catalog("workspace");
        let request = core.next_skills_read().unwrap();
        assert!(!request.management);
        let manager = core.acquire_skills_demand("workspace", None);
        assert!(core.next_skills_read().is_none());
        core.complete_skills_catalog(request, Ok(response()));
        assert_eq!(
            read.wait_while(|| true).unwrap().request,
            SkillsLoadState::Ready
        );
        let upgraded = core.next_skills_read().unwrap();
        assert!(upgraded.management);
        drop(manager);
        let before = core.skills_catalog_snapshot("workspace").unwrap();
        core.complete_skills_catalog(upgraded, Ok(response()));
        assert!(Arc::ptr_eq(
            &before,
            &core.skills_catalog_snapshot("workspace").unwrap()
        ));
    }
}
