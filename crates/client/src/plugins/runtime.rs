//! Shared Plugins catalog and management over immutable scoped publications.
use crate::core::{ClientCore, ClientMutationAuthority, ClientScope, ClientTransition};
use pioneer_protocol::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PluginIntent {
    Observe {
        plugin_id: Option<String>,
    },
    Close,
    SetEnabled {
        plugin_id: String,
        expected_revision: i64,
        enabled: bool,
    },
    Mutate {
        plugin_id: String,
        expected_revision: i64,
        intent: PluginManagementIntent,
    },
    Install {
        upload_operation: u64,
    },
    ApplyUpdate {
        upload_operation: u64,
        confirm_changes: bool,
    },
    Skill {
        skill_id: SkillId,
        enabled: bool,
        allow_implicit_invocation: bool,
    },
    RemoveSkill {
        skill_id: SkillId,
    },
    ConfigureMcp {
        server_id: String,
        body: String,
    },
    Mcp {
        intent: crate::mcp::operations::McpIntent,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
pub struct PluginPublication {
    pub operations: BTreeMap<String, PluginOperationPublication>,
    pub loaded: bool,
    pub connection_id: Option<u64>,
    pub revision: u64,
    pub generation: u64,
    pub plugin_id: Option<String>,
    pub plugins: Vec<PluginItem>,
    pub details: Option<PluginItem>,
    pub skills: Vec<SkillListItem>,
    pub skill_health: Vec<SkillHealthItem>,
    pub mcp: Vec<McpListItem>,
    pub mcp_details: Vec<McpServerDetailsResponse>,
    pub oauth: Vec<PluginOAuthPresentation>,
    pub busy: bool,
    pub managing: bool,
    /// Failure of the shared catalog read. Operation failures are keyed below.
    pub error: Option<String>,
    /// A dispatched mutation failed without an authoritative response. Refresh
    /// or retry using the retained upload identity; never infer rollback.
    pub mutation_uncertain: bool,
}
impl PluginPublication {
    /// Reduce the completed work's keyed outcome independently of catalog reads.
    fn settle_operation(
        &mut self,
        key: &str,
        result: anyhow::Result<Self>,
        mutation_dispatched: bool,
        catalog_read_succeeded: Option<bool>,
        retained_parents: &std::collections::BTreeSet<String>,
    ) {
        let failed = result.as_ref().err().map(|error| format!("{error:#}"));
        if let Some(operation) = self.operations.get_mut(key) {
            operation.busy = false;
            operation.uncertain =
                mutation_dispatched && operation.result.is_none() && failed.is_some();
            operation.error = failed.clone();
        }
        if let Ok(mut next) = result {
            if key.is_empty() {
                next.mutation_uncertain = self.mutation_uncertain;
            }
            next.operations = std::mem::take(&mut self.operations);
            *self = next;
        }
        // A shared read error applies to every surface. A command/admission
        // error belongs only to its key, including native commands without a
        // PluginsMutationResponse. A failed command cannot erase an older read
        // error; a successful catalog read clears it.
        match catalog_read_succeeded {
            Some(true) => self.error = None,
            Some(false) => self.error = failed,
            None if key.is_empty() => self.error = failed,
            None => {}
        }
        if let Some(operation) = self.operations.get(key).cloned() {
            // Install starts under an upload key; only this work's typed
            // response can alias its outcome to the authoritative parent ID.
            if let Some(parent) = operation
                .result
                .as_ref()
                .and_then(|result| result.plugin.as_ref())
            {
                if self
                    .operations
                    .get(&parent.id)
                    .is_none_or(|operation| !operation.busy)
                {
                    self.operations.insert(parent.id.clone(), operation.clone());
                }
            }
            self.mutation_uncertain = operation.uncertain;
        }
        let live_parents = self
            .plugins
            .iter()
            .map(|p| p.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        self.operations.retain(|operation_key, operation| {
            operation.busy
                || operation.uncertain
                || operation_key == key
                || live_parents.contains(operation_key.as_str())
                || retained_parents.contains(operation_key)
        });
        self.busy = self.operations.values().any(|operation| operation.busy);
    }
}
/// Captured connection and effective authorization at the shell submission.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PluginCommandOrigin {
    pub connection_generation: u64,
    pub authorization_sequence: u64,
    pub connection_id: Option<u64>,
}
impl PluginCommandOrigin {
    fn epoch(&self) -> (u64, u64, Option<u64>) {
        (
            self.connection_generation,
            self.authorization_sequence,
            self.connection_id,
        )
    }
    fn from_epoch(epoch: (u64, u64, Option<u64>)) -> Self {
        Self {
            connection_generation: epoch.0,
            authorization_sequence: epoch.1,
            connection_id: epoch.2,
        }
    }
}
/// Admission identity, captured from this command's immutable publication.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PluginCommandReceipt {
    pub parent_id: String,
    pub generation: u64,
    pub origin: PluginCommandOrigin,
}
/// A parent's operation outcome is independent of other visible parents.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize)]
pub struct PluginOperationPublication {
    pub generation: u64,
    pub origin: PluginCommandOrigin,
    pub native_operation_id: Option<u64>,
    pub busy: bool,
    pub result: Option<PluginsMutationResponse>,
    pub uncertain: bool,
    pub error: Option<String>,
}
/// Secret-free native authorization presentation, shared by management shells.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct PluginOAuthPresentation {
    pub server_id: String,
    pub flow_id: Option<String>,
    pub state: McpOAuthState,
    pub browser_failed: bool,
}
#[derive(Clone)]
struct Work {
    work_context: pioneer_observability::work::WorkCarrier,
    workspace: String,
    key: String,
    generation: u64,
    plugin_id: Option<String>,
    epoch: (u64, u64, Option<u64>),
    intent: PluginIntent,
}
#[derive(Default)]
pub(crate) struct PluginController {
    requests: BTreeMap<(String, String), crate::request_state::RequestState<()>>,
    // Ordinary parent commands only. A fresh Observe enables an explicit CAS
    // recovery; it never resolves their unknown original outcome.
    recovery_reads: BTreeMap<(String, String), (u64, Option<i64>)>,
    next_consumer: u64,
    consumers: BTreeMap<String, BTreeMap<u64, Option<String>>>,
    pending_observations: BTreeMap<String, (Option<String>, PluginCommandOrigin)>,
    suspended: std::collections::BTreeSet<String>,
    refresh_requested: std::collections::BTreeSet<String>,
    sender: Option<mpsc::SyncSender<Work>>,
    observer: Option<pioneer_observability::queues::QueueObserver>,
    task: Option<std::thread::JoinHandle<()>>,
    publications: BTreeMap<String, PluginPublication>,
}
impl PluginController {
    // Called with the owner lock held by both shell eligibility and admission.
    fn parent_recovery_available(
        &self,
        workspace: &str,
        parent: &str,
        epoch: (u64, u64, Option<u64>),
    ) -> bool {
        let Some((generation, Some(revision))) = self.recovery_reads.get(&(workspace.into(), parent.into())) else { return false; };
        self.requests.get(&(workspace.into(), String::new())).is_some_and(|request| {
            matches!(request.status(), crate::request_state::RequestStatus::Ready { .. })
        }) && self.publications.get(workspace).is_some_and(|publication| {
            publication.loaded && publication.error.is_none() && publication.connection_id == epoch.2
                && publication.plugins.iter().any(|item| item.id == parent && item.revision == *revision)
                && publication.operations.get(parent).is_some_and(|operation| {
                    operation.generation == *generation && operation.origin.epoch() == epoch
                        && operation.uncertain && !operation.busy
                })
        })
    }
    fn reconcile_recovery_reads(&mut self, workspace: &str) {
        let Some(publication) = self.publications.get(workspace) else { return; };
        for ((ws, parent), (generation, revision)) in &mut self.recovery_reads {
            if ws == workspace && publication.operations.get(parent).is_some_and(|operation| {
                operation.generation == *generation && operation.uncertain && !operation.busy
            }) {
                *revision = publication.plugins.iter().find(|item| &item.id == parent).map(|item| item.revision);
            }
        }
    }
    pub(crate) fn stop(&mut self) {
        self.recovery_reads.clear();
        self.sender.take();
        self.publications.clear();
        for request in self.requests.values_mut() {
            request.cancel(request.latest_generation());
        }
        self.consumers.clear();
        self.pending_observations.clear();
        self.refresh_requested.clear();
        self.suspended.clear();
    }
}
impl Drop for PluginController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
/// A visible catalog/details consumer, using the same retained-demand pattern
/// as SkillsDemand and McpDemand. Dropping a lease never erases published data.
pub type PluginDemand = crate::request_state::CatalogDemand;
impl ClientCore {
    /// A fresh authorized Observe enables an explicit recovery against its CAS.
    /// The old operation remains uncertain. Upload retries use their own exact
    /// upload/fingerprint flow and are never admitted through this path.
    pub fn plugin_parent_recovery_available(&self, workspace: &str, parent: &str) -> bool {
        let epoch = self.provider_runtime_epoch();
        let owner = self.plugins_controller.lock().expect("plugins poisoned");
        owner.parent_recovery_available(workspace, parent, epoch)
    }

    pub fn plugin_command_origin(&self) -> PluginCommandOrigin {
        PluginCommandOrigin::from_epoch(self.provider_runtime_epoch())
    }
    /// Bind a shell's explicit parent and authorization origin before enqueue.
    /// The returned identity is admission, never a completed native save.
    pub fn submit_plugin_command(
        &self,
        workspace: &str,
        parent: &str,
        intent: PluginIntent,
        origin: &PluginCommandOrigin,
    ) -> anyhow::Result<PluginCommandReceipt> {
        anyhow::ensure!(
            !matches!(
                intent,
                PluginIntent::Observe { .. } | PluginIntent::Close | PluginIntent::Install { .. }
            ),
            "plugin_parent_command_required"
        );
        let transition = self.plugin_intent_bound(
            workspace,
            intent,
            self.plugins_management_allowed(workspace),
            Some(parent.into()),
            Some(origin),
            false,
        );
        anyhow::ensure!(
            transition.outcome() == crate::core::ClientTransitionOutcome::Changed,
            "plugin_command_rejected"
        );
        transition
            .changes()
            .publications()
            .iter()
            .find_map(|reference| {
                let publication = reference.snapshot().payload::<PluginPublication>()?;
                let operation = publication.operations.get(parent)?;
                (operation.origin == *origin).then(|| PluginCommandReceipt {
                    parent_id: parent.into(),
                    generation: operation.generation,
                    origin: origin.clone(),
                })
            })
            .ok_or_else(|| anyhow::anyhow!("plugin_command_rejected"))
    }
    pub fn plugin_command_operation(
        &self,
        workspace: &str,
        receipt: &PluginCommandReceipt,
    ) -> Option<PluginOperationPublication> {
        if self.plugin_command_origin() != receipt.origin {
            return None;
        }
        let publication = self.plugin_details_publication(workspace, &receipt.parent_id)?;
        publication
            .operations
            .get(&receipt.parent_id)
            .filter(|operation| {
                operation.generation == receipt.generation && operation.origin == receipt.origin
            })
            .cloned()
    }
    pub fn acquire_plugin_demand(
        self: &Arc<Self>,
        workspace: &str,
        plugin_id: Option<&str>,
    ) -> PluginDemand {
        let epoch = self.provider_runtime_epoch();
        let mut owner = self.plugins_controller.lock().expect("plugins poisoned");
        owner.next_consumer = owner
            .next_consumer
            .checked_add(1)
            .expect("plugin demand exhausted");
        let identity = owner.next_consumer;
        owner
            .consumers
            .entry(workspace.into())
            .or_default()
            .insert(identity, plugin_id.map(str::to_owned));
        owner.suspended.remove(workspace);
        let dirty = owner.refresh_requested.remove(workspace);
        let pending = owner.pending_observations.remove(workspace).filter(|(_, origin)| origin.epoch() == epoch).map(|(id, _)| id);
        let refresh = dirty
            || pending.is_some()
            || owner.publications.get(workspace).is_none_or(|p| {
                p.connection_id != epoch.2 || ((!p.loaded || p.error.is_some()) && !p.busy)
            });
        drop(owner);
        if refresh {
            self.plugin_intent(
                workspace,
                PluginIntent::Observe {
                    plugin_id: plugin_id.map(str::to_owned),
                },
            );
        }
        let core = Arc::downgrade(self);
        let workspace = workspace.to_owned();
        crate::request_state::CatalogDemand::new(identity, move || {
            if let Some(core) = core.upgrade() {
                let mut owner = core.plugins_controller.lock().expect("plugins poisoned");
                if let Some(consumers) = owner.consumers.get_mut(&workspace) {
                    consumers.remove(&identity);
                    if consumers.is_empty() {
                        owner.consumers.remove(&workspace);
                        if core
                            .current_scope_demand(&ClientScope::Plugins {
                                workspace_id: workspace.clone(),
                            })
                            .is_none_or(|demand| demand == crate::core::ClientDemand::Suspended)
                        {
                            owner.suspended.insert(workspace);
                        }
                    }
                }
            }
        })
    }

    fn publish_plugins(&self, workspace: &str, p: &mut PluginPublication) -> ClientTransition {
        let scope = ClientScope::Plugins {
            workspace_id: workspace.into(),
        };
        p.revision = self
            .snapshot(&scope)
            .map_or(0, |s| s.revisions().scoped().get())
            .saturating_add(1);
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(p.revision),
            Arc::new(p.clone()),
            vec![],
        )
    }
    /// A surface selects its own parent from the shared inventory. Selection
    /// does not issue a request or replace another consumer's details.
    pub fn plugin_details_publication(
        &self,
        workspace: &str,
        plugin_id: &str,
    ) -> Option<PluginPublication> {
        let epoch = self.provider_runtime_epoch();
        let scope = ClientScope::Plugins {
            workspace_id: workspace.into(),
        };
        let publication: Arc<PluginPublication> = self.snapshot(&scope)?.snapshot().payload()?;
        let mut publication = (*publication).clone();
        publication.details = publication
            .plugins
            .iter()
            .find(|p| p.id == plugin_id)
            .cloned();
        publication.plugin_id = Some(plugin_id.to_owned());
        let operation = publication
            .operations
            .get(plugin_id)
            .cloned()
            .unwrap_or_default();
        publication.busy = operation.busy;
        publication.mutation_uncertain = operation.uncertain;
        if operation.error.is_some() {
            publication.error = operation.error;
        }
        self.project_plugin_native_snapshots(workspace, &mut publication);
        (self.provider_runtime_epoch() == epoch && publication.connection_id == epoch.2)
            .then_some(publication)
    }
    /// Compose only already authorized native snapshots. Native views/bridges
    /// own demand and refresh; parent loading never starts their request graph.
    fn project_plugin_native_snapshots(
        &self,
        workspace: &str,
        publication: &mut PluginPublication,
    ) {
        publication.skills.clear();
        publication.skill_health.clear();
        publication.mcp.clear();
        publication.mcp_details.clear();
        publication.oauth.clear();
        let Some(parent) = &publication.details else {
            return;
        };
        if let Some(skills) = self.skills_catalog_snapshot(workspace) {
            publication.skills = skills
                .catalog
                .iter()
                .filter(|s| {
                    parent
                        .components
                        .iter()
                        .any(|c| c.skill_id.as_ref() == Some(&s.skill_id))
                })
                .map(|s| (**s).clone())
                .collect();
        }
        for skill in &publication.skills {
            if let Some(details) = self.skills_details_snapshot(workspace, &skill.skill_id) {
                if let Some(health) = &details.health {
                    publication.skill_health.push((**health).clone());
                }
            }
        }
        if let Some(mcp) = self.mcp_catalog_snapshot(workspace) {
            publication.mcp = mcp
                .servers()
                .iter()
                .filter(|s| {
                    parent
                        .components
                        .iter()
                        .any(|c| c.mcp_installation_id.as_deref() == Some(s.id.as_str()))
                })
                .map(|s| (**s).clone())
                .collect();
        }
        for server in &publication.mcp {
            let snapshot = self.mcp_details_snapshot(workspace, &server.id);
            let details = snapshot.as_ref().and_then(|s| s.details());
            let management = details
                .and_then(|d| d.management.as_ref())
                .and_then(|m| m.oauth_state);
            if let Some(details) = details {
                publication.mcp_details.push((**details).clone());
            }
            if let Some((event, browser_failed)) =
                self.mcp_oauth_presentation(workspace, &server.id)
            {
                let callback_unavailable = event.flow_id.is_some()
                    && event.diagnostic.as_deref() == Some("oauth_callback_unavailable");
                let state = crate::mcp::oauth::effective_oauth_management_state(
                    Some(event.state),
                    management,
                    callback_unavailable,
                )
                .unwrap_or(event.state);
                let flow_id = (state == event.state).then_some(event.flow_id).flatten();
                publication.oauth.push(PluginOAuthPresentation {
                    server_id: server.id.clone(),
                    flow_id,
                    state,
                    browser_failed,
                });
            }
        }
    }

    /// Native child commands bind the originating surface's parent explicitly.
    pub fn plugin_intent_for(
        &self,
        workspace: &str,
        plugin_id: &str,
        intent: PluginIntent,
    ) -> ClientTransition {
        let managing = self.plugins_management_allowed(workspace);
        self.plugin_intent_bound(workspace, intent, managing, Some(plugin_id.to_owned()), None, false)
    }
    pub fn plugin_intent(&self, workspace: &str, intent: PluginIntent) -> ClientTransition {
        let managing = self.plugins_management_allowed(workspace);
        self.plugin_intent_with_management(workspace, intent, managing)
    }
    fn plugin_intent_with_management(
        &self,
        workspace: &str,
        intent: PluginIntent,
        managing: bool,
    ) -> ClientTransition {
        self.plugin_intent_bound(workspace, intent, managing, None, None, false)
    }
    fn plugin_intent_bound(
        &self,
        workspace: &str,
        intent: PluginIntent,
        managing: bool,
        parent: Option<String>,
        origin: Option<&PluginCommandOrigin>,
        continuing_read: bool,
    ) -> ClientTransition {
        // I -> P matches identity publication. No RPC runs in this guard.
        let _identity = self.identity_authorization.lock().expect("identity owner poisoned");
        let managing = managing && self.plugins_management_allowed(workspace);
        if matches!(&intent, PluginIntent::Observe { .. })
            && (self.is_stopped() || self.committed_capability_snapshot(workspace).is_none())
        {
            return self.reject_intent();
        }
        let epoch = self.provider_runtime_epoch();
        let mut owner = self.plugins_controller.lock().expect("plugins poisoned");
        if origin.is_some_and(|origin| origin.epoch() != epoch || self.provider_runtime_epoch() != epoch) {
            return self.reject_intent();
        }
        // Updates must retain an upload-owner operation for confirmation and
        // exact fingerprint recovery. A raw Update bypasses that ownership.
        if matches!(&intent, PluginIntent::Mutate { intent: PluginManagementIntent::Update { .. }, .. }) {
            return self.reject_intent();
        }
        let observing = matches!(intent, PluginIntent::Observe { .. });
        if continuing_read {
            let PluginIntent::Observe { plugin_id } = &intent else { return self.reject_intent(); };
            // Recheck under the same lock as admission: Close/auth/release or a
            // newer selection may have superseded the captured continuation.
            if owner.pending_observations.get(workspace).is_some_and(|(pending, captured)| pending != plugin_id || captured.epoch() != epoch)
                || (!owner.pending_observations.contains_key(workspace) && !owner.refresh_requested.contains(workspace))
            {
                return self.reject_intent();
            }
        }
        if owner.suspended.contains(workspace) && !matches!(intent, PluginIntent::Close) {
            return self.reject_intent();
        }
        if observing && owner.publications.get(workspace).is_some_and(|p| p.busy) {
            if let PluginIntent::Observe { plugin_id } = &intent {
                owner
                    .pending_observations
                    .insert(workspace.into(), (plugin_id.clone(), PluginCommandOrigin::from_epoch(epoch)));
            }
            let p = owner.publications.get_mut(workspace).unwrap();
            return self.publish_plugins(workspace, p);
        }
        if !observing
            && !matches!(intent, PluginIntent::Close)
            && !owner.publications.contains_key(workspace)
        {
            return self.reject_intent();
        }
        if matches!(intent, PluginIntent::Close) {
            if owner.consumers.contains_key(workspace)
                || owner
                    .publications
                    .get(workspace)
                    .is_some_and(|p| p.operations.values().any(|o| o.busy))
            {
                let p = owner.publications.entry(workspace.into()).or_default();
                return self.publish_plugins(workspace, p);
            }
            owner.pending_observations.remove(workspace);
            owner.refresh_requested.remove(workspace);
            let p = owner.publications.entry(workspace.into()).or_default();
            p.busy = false;
            let out = self.publish_plugins(workspace, p);
            owner.publications.remove(workspace);
            return out;
        }
        let empty_publication = PluginPublication::default();
        let p = owner.publications.get(workspace).unwrap_or(&empty_publication);
        let operation_parent = match &intent {
            PluginIntent::Install { .. } => None,
            PluginIntent::Observe { plugin_id } => plugin_id.clone(),
            PluginIntent::SetEnabled { plugin_id, .. } | PluginIntent::Mutate { plugin_id, .. } => Some(plugin_id.clone()),
            PluginIntent::ApplyUpdate { upload_operation, .. } => self.skill_upload_snapshot(workspace, *upload_operation).and_then(|p| match &p.target {
                crate::package_delivery::operations::SkillUploadTarget::PluginUpdatePreview { plugin_id, .. } => Some(plugin_id.clone()), _ => None,
            }),
            _ => parent.clone().or_else(|| p.plugin_id.clone()),
        };
        if parent.as_ref().is_some_and(|parent| operation_parent.as_ref() != Some(parent)) {
            return self.reject_intent();
        }
        let key = if observing {
            String::new()
        } else if let PluginIntent::Install { upload_operation } = &intent {
            format!("upload:{upload_operation}")
        } else {
            operation_parent.clone().unwrap_or_default()
        };
        let parent_busy = operation_parent.as_ref().is_some_and(|id| {
            p.operations.values().any(|operation| {
                operation.busy
                    && operation
                        .result
                        .as_ref()
                        .and_then(|r| r.plugin.as_ref())
                        .is_some_and(|parent| &parent.id == id)
            })
        });
        if self.is_stopped()
            || (!observing
                && (key.is_empty()
                    || !managing
                    || parent_busy
                    || p.operations.get(&key).is_some_and(|o| o.busy)))
        {
            return self.reject_intent();
        }
        match &intent {
            PluginIntent::SetEnabled {
                plugin_id,
                expected_revision,
                ..
            }
            | PluginIntent::Mutate {
                plugin_id,
                expected_revision,
                ..
            } if !p
                .plugins
                .iter()
                .any(|d| &d.id == plugin_id && d.revision == *expected_revision) =>
            {
                return self.reject_intent();
            }
            PluginIntent::Skill { .. }
            | PluginIntent::RemoveSkill { .. }
            | PluginIntent::ConfigureMcp { .. }
            | PluginIntent::Mcp { .. }
                if !p
                    .plugins
                    .iter()
                    .any(|d| Some(&d.id) == parent.as_ref().or(p.plugin_id.as_ref())) =>
            {
                return self.reject_intent();
            }
            _ => {}
        }
        let ordinary_parent_command = matches!(&intent, PluginIntent::SetEnabled { .. } | PluginIntent::Mutate {
            intent: PluginManagementIntent::Continue | PluginManagementIntent::Retry { .. }
                | PluginManagementIntent::Remove { .. }, ..
        });
        let uncertain_origin = p.operations.get(&key).filter(|operation| operation.uncertain).map(|operation| operation.origin.epoch());
        if let Some(uncertain_origin) = uncertain_origin {
            // Check before replacing generation, read evidence or the outcome.
            // Observe never acknowledges the old mutation; it only permits an
            // explicit ordinary command using the observed parent CAS above.
            let recovery_revision = match &intent {
                PluginIntent::SetEnabled { expected_revision, .. }
                | PluginIntent::Mutate { expected_revision, .. } => Some(*expected_revision),
                PluginIntent::ApplyUpdate { upload_operation, .. } => self.skill_upload_snapshot(workspace, *upload_operation).and_then(|upload| match &upload.target {
                    crate::package_delivery::operations::SkillUploadTarget::PluginUpdatePreview { plugin_id, expected_revision } if plugin_id == &key => Some(*expected_revision),
                    _ => None,
                }),
                _ => None,
            };
            let recovery = owner.parent_recovery_available(workspace, &key, epoch)
                && owner.recovery_reads.get(&(workspace.into(), key.clone())).is_some_and(|(_, revision)| *revision == recovery_revision);
            let upload_retry = uncertain_origin == epoch
                && !owner.recovery_reads.contains_key(&(workspace.into(), key.clone()))
                && match &intent {
                    PluginIntent::Install { upload_operation } => self.skill_upload_snapshot(workspace, *upload_operation).is_some_and(|upload| {
                        upload.mutation_uncertain && upload.plugin_preview.is_some()
                            && matches!(&upload.target, crate::package_delivery::operations::SkillUploadTarget::PluginInstall | crate::package_delivery::operations::SkillUploadTarget::PluginPreview)
                    }),
                    PluginIntent::ApplyUpdate { upload_operation, .. } => self.skill_upload_snapshot(workspace, *upload_operation).is_some_and(|upload| {
                        upload.mutation_uncertain && upload.plugin_update_preview.is_some()
                            && matches!(&upload.target, crate::package_delivery::operations::SkillUploadTarget::PluginUpdatePreview { plugin_id, .. } if plugin_id == &key)
                    }),
                    _ => false,
                };
            // The retained upload owner alone supplies an exact upload and
            // fingerprint retry. Native/new upload commands cannot replace it.
            if !recovery && !upload_retry {
                return self.reject_intent();
            }
        }
        // Reserve only a prospective request locally. Failed enqueue exposes
        // no generation; a worker cannot pass its fence until this lock releases.
        let mut request = owner.requests.get(&(workspace.into(), key.clone())).cloned().unwrap_or_default();
        let generation = request.start().get();
        let observed_parent = match &intent {
            PluginIntent::Observe { plugin_id } => Some(plugin_id.clone()),
            _ => None,
        };
        let work = Work {
            work_context: pioneer_observability::work::WorkCarrier::child(
                pioneer_observability::work::Boundary::StdThread,
                pioneer_observability::work::WorkKind::ClientCommand,
                pioneer_observability::work::Trigger::Event,
                pioneer_observability::work::ExecutorRole::ClientController,
            ),
            workspace: workspace.into(),
            key: key.clone(),
            generation,
            plugin_id: operation_parent,
            epoch,
            intent,
        };
        if pioneer_observability::queues::observe_try_send(
            owner.observer.as_ref(),
            pioneer_observability::queues::QueueBytes::Unsupported,
            || {
                owner
                    .sender
                    .as_ref()
                    .ok_or(())?
                    .try_send(work)
                    .map_err(|_| ())
            },
        )
        .is_err()
        {
            if let Some(plugin_id) = observed_parent {
                owner.pending_observations.insert(workspace.into(), (plugin_id, PluginCommandOrigin::from_epoch(epoch)));
            }
            return self.reject_intent();
        }
        if observing {
            // Direct reads supersede older selection; continuations rechecked
            // that they still own the latest obligation under this same lock.
            owner.pending_observations.remove(workspace);
            owner.refresh_requested.remove(workspace);
        }
        owner.requests.insert((workspace.into(), key.clone()), request);
        owner.recovery_reads.remove(&(workspace.into(), key.clone()));
        if ordinary_parent_command {
            owner.recovery_reads.insert((workspace.into(), key.clone()), (generation, None));
        }
        let p = owner.publications.entry(workspace.into()).or_default();
        // Observing during a mutation cannot supersede its completion. A screen
        // close fences presentation, but never pretends to cancel server work.
        if let Some(plugin_id) = &observed_parent {
            if p.plugin_id != *plugin_id {
                p.details = None;
                p.skills.clear();
                p.skill_health.clear();
                p.mcp.clear();
                p.mcp_details.clear();
                p.oauth.clear();
            }
            p.plugin_id = plugin_id.clone();
        }
        p.connection_id = epoch.2;
        p.generation = generation;
        p.busy = true;
        if !observing {
            p.operations.insert(
                key.clone(),
                PluginOperationPublication {
                    generation,
                    origin: PluginCommandOrigin::from_epoch(epoch),
                    busy: true,
                    ..Default::default()
                },
            );
            p.mutation_uncertain = false;
        }
        p.managing = managing;
        self.publish_plugins(workspace, p)
    }

    fn plugin_work_current(&self, work: &Work) -> bool {
        let epoch = self.provider_runtime_epoch();
        let owner = self.plugins_controller.lock().expect("plugins poisoned");
        !self.is_stopped() && epoch == work.epoch
            && owner.publications.contains_key(&work.workspace)
            && owner.requests.get(&(work.workspace.clone(),work.key.clone())).is_some_and(|r| matches!(r.status(), crate::request_state::RequestStatus::Pending { generation, .. } if generation.get()==work.generation))
    }
    fn plugin_work_parent(&self, work: &Work) -> Option<Option<String>> {
        self.plugin_work_current(work)
            .then(|| work.plugin_id.clone())
    }
    // Only bounded native admission runs under this short guard: no RPC, wait,
    // filesystem or browser effect. Close/reopen cannot replace the parent before
    // enqueue. Once admitted, native work owns its original epoch independently.
    fn admit_plugin_child(
        &self,
        work: &Work,
        admit: impl FnOnce() -> anyhow::Result<u64>,
    ) -> anyhow::Result<u64> {
        let owner = self.plugins_controller.lock().expect("plugins poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && self.provider_runtime_epoch() == work.epoch
                && owner
                    .publications
                    .get(&work.workspace)
                    .is_some_and(|p| p.operations.get(&work.key).is_some_and(|o| o.busy)),
            "plugin_catalog_stale"
        );
        admit()
    }
    pub(crate) fn refresh_plugins_for_native_notification(
        &self,
        notification: &GatewayNotification,
    ) {
        let (workspace, skill_ids, server_id, names) = match notification {
            GatewayNotification::SkillsChanged(event) => (
                event.workspace_id.as_str(),
                Some(
                    event
                        .changes
                        .iter()
                        .map(|change| &change.skill_id)
                        .collect::<Vec<_>>(),
                ),
                None,
                None,
            ),
            GatewayNotification::McpServerStatusChanged(event) => (
                event.workspace_id.as_str(),
                None,
                Some(event.server.id.as_str()),
                None,
            ),
            GatewayNotification::McpOAuthChanged(event) => (
                event.workspace_id.as_str(),
                None,
                Some(event.server_id.as_str()),
                None,
            ),
            GatewayNotification::McpChanged(event) => (
                event.workspace_id.as_str(),
                None,
                None,
                Some(
                    event
                        .changed
                        .iter()
                        .map(|change| change.name.as_str())
                        .collect::<Vec<_>>(),
                ),
            ),
            // Tool/resource catalog counts are native detail state. They do not
            // change parent ownership, revision or component runtime status.
            _ => return,
        };
        let affected = {
            let owner = self.plugins_controller.lock().expect("plugins poisoned");
            owner.publications.get(workspace).is_none_or(|p| {
                if !p.loaded {
                    return true;
                }
                if let Some(ids) = skill_ids {
                    ids.is_empty()
                        || p.plugins
                            .iter()
                            .flat_map(|p| &p.components)
                            .any(|component| {
                                component
                                    .skill_id
                                    .as_ref()
                                    .is_some_and(|id| ids.contains(&id))
                            })
                } else if let Some(id) = server_id {
                    p.plugins
                        .iter()
                        .flat_map(|p| &p.components)
                        .any(|component| component.mcp_installation_id.as_deref() == Some(id))
                } else if let Some(names) = names {
                    // Name-only legacy notifications cannot identify a child
                    // missing from our native projection. Preserve invalidation
                    // in that case rather than infer that it is unrelated.
                    names.is_empty()
                        || p.plugins
                            .iter()
                            .flat_map(|p| &p.components)
                            .filter_map(|c| c.mcp_installation_id.as_ref())
                            .any(|id| !p.mcp.iter().any(|server| &server.id == id))
                        || p.mcp
                            .iter()
                            .any(|server| names.contains(&server.name.as_str()))
                } else {
                    false
                }
            })
        };
        if affected {
            self.refresh_plugin_publication(workspace);
        }
    }

    pub(crate) fn refresh_plugin_publication(&self, workspace: &str) {
        self.refresh_composer_plugins(workspace);
        let id = {
            let mut owner = self.plugins_controller.lock().expect("plugins poisoned");
            let Some(p) = owner.publications.get(workspace) else {
                return;
            };
            if p.busy || owner.suspended.contains(workspace) {
                owner.refresh_requested.insert(workspace.into());
                return;
            }
            p.plugin_id.clone()
        };
        self.plugin_intent(workspace, PluginIntent::Observe { plugin_id: id });
    }
    pub(crate) fn invalidate_plugin_publications(&self) {
        let mut owner = self.plugins_controller.lock().expect("plugins poisoned");
        let workspaces = owner.publications.keys().chain(owner.pending_observations.keys())
            .chain(owner.consumers.keys()).cloned().collect::<std::collections::BTreeSet<_>>();
        // Old authorization must not carry a deferred selected read forward.
        owner.pending_observations.clear();
        owner.recovery_reads.clear();
        for request in owner.requests.values_mut() {
            request.cancel(request.latest_generation());
        }
        for workspace in workspaces {
            let request = owner
                .requests
                .entry((workspace.clone(), String::new()))
                .or_default();
            let generation = request.start().get();
            request.cancel(request.latest_generation());
            let p = owner.publications.entry(workspace.clone()).or_default();
            let id = p.plugin_id.clone();
            // The enclosing identity transaction evicts published values. Fence
            // workers here; resume only after the new authorization is committed.
            *p = PluginPublication {
                generation,
                plugin_id: id,
                ..Default::default()
            };
            owner.refresh_requested.insert(workspace);
        }
    }
    pub(crate) fn resume_plugin_publications(&self) {
        let resume = {
            let owner = self.plugins_controller.lock().expect("plugins poisoned");
            owner
                .publications
                .iter()
                .filter(|(workspace, _)| !owner.suspended.contains(*workspace))
                .map(|(workspace, p)| (workspace.clone(), p.plugin_id.clone()))
                .collect::<Vec<_>>()
        };
        for (workspace, plugin_id) in resume {
            self.plugins_controller
                .lock()
                .expect("plugins poisoned")
                .refresh_requested
                .remove(&workspace);
            // Identity callers resume after releasing I. A delayed previous
            // resume is serialized again at Observe admission against the
            // latest committed authorization, never against a precommit A0.
            self.plugin_intent_with_management(
                &workspace,
                PluginIntent::Observe { plugin_id },
                false,
            );
        }
    }
    pub(crate) fn plugin_demand_changed(
        &self,
        scope: &ClientScope,
        demand: crate::core::ClientDemand,
    ) {
        let ClientScope::Plugins { workspace_id } = scope else {
            return;
        };
        let demand = self.current_scope_demand(scope).unwrap_or(demand);
        let epoch = self.provider_runtime_epoch();
        let mut owner = self.plugins_controller.lock().expect("plugins poisoned");
        if demand == crate::core::ClientDemand::Suspended {
            if !owner.consumers.contains_key(workspace_id) {
                owner.suspended.insert(workspace_id.clone());
            }
            return;
        }
        owner.suspended.remove(workspace_id);
        let dirty = owner.refresh_requested.remove(workspace_id);
        let pending = owner.pending_observations.remove(workspace_id).filter(|(_, origin)| origin.epoch() == epoch).map(|(id, _)| id);
        let publication = owner.publications.get(workspace_id);
        let refresh = dirty
            || pending.is_some()
            || publication.is_none_or(|p| (!p.loaded || p.error.is_some()) && !p.busy);
        let id = pending.unwrap_or_else(|| publication.and_then(|p| p.plugin_id.clone()));
        drop(owner);
        if refresh {
            self.plugin_intent(workspace_id, PluginIntent::Observe { plugin_id: id });
        }
    }
    // One finite pass per receiver capacity release/settlement. Failed sends
    // retain their obligation; a full queue guarantees another receiver turn.
    fn resume_pending_plugin_reads(&self) {
        let origin = self.plugin_command_origin();
        if self.is_stopped() || origin.connection_id.is_none() { return; }
        let reads = {
            let owner = self.plugins_controller.lock().expect("plugins poisoned");
            owner.pending_observations.keys().chain(owner.refresh_requested.iter())
                .filter(|workspace| !owner.suspended.contains(*workspace))
                .filter_map(|workspace| {
                    let publication = owner.publications.get(workspace);
                    if owner.pending_observations.get(workspace).is_some_and(|(_, captured)| captured != &origin)
                        || publication.is_some_and(|p| p.busy) { return None; }
                    // Auth invalidation's refresh marker is resumed explicitly
                    // after identity commit, never by an old receiver turn.
                    if !owner.pending_observations.contains_key(workspace)
                        && publication.is_none_or(|p| p.connection_id != origin.connection_id) { return None; }
                    let selected = owner.pending_observations.get(workspace).map(|(selected, _)| selected.clone())
                        .unwrap_or_else(|| publication.and_then(|p| p.plugin_id.clone()));
                    Some((workspace.clone(), selected))
                }).collect::<BTreeMap<_, _>>()
        };
        for (workspace, plugin_id) in reads {
            self.plugin_intent_bound(&workspace, PluginIntent::Observe { plugin_id }, false, None, Some(&origin), true);
        }
    }
    fn settle_plugin_work(
        &self,
        work: &Work,
        result: anyhow::Result<PluginPublication>,
        mutation_dispatched: bool,
        catalog_read_succeeded: Option<bool>,
    ) {
        let succeeded = result.is_ok();
        let _identity=self.identity_authorization.lock().expect("identity owner poisoned");
        if !self.plugin_work_current(work){return;}
        let mut owner=self.plugins_controller.lock().expect("plugins poisoned");
        if !owner.requests.get(&(work.workspace.clone(),work.key.clone())).is_some_and(|r| matches!(r.status(),crate::request_state::RequestStatus::Pending { generation,.. } if generation.get()==work.generation)){return;}
        let retained_parents=owner.consumers.get(&work.workspace).into_iter().flat_map(|c|c.values()).filter_map(|id|id.clone()).collect::<std::collections::BTreeSet<_>>();
        let observed = matches!(&work.intent, PluginIntent::Observe { .. }) && result.is_ok();
        if let Some(p)=owner.publications.get_mut(&work.workspace){
            p.settle_operation(&work.key,result,mutation_dispatched,catalog_read_succeeded,&retained_parents);
        }
        if observed { owner.reconcile_recovery_reads(&work.workspace); }
        if let Some(p)=owner.publications.get_mut(&work.workspace){ self.publish_plugins(&work.workspace,p); }
        if let Some(request)=owner.requests.get_mut(&(work.workspace.clone(),work.key.clone())) {
            let generation=crate::core::ClientGeneration::new(work.generation);
            if succeeded {
                request.succeed(generation, ());
            } else {
                request.fail(generation, crate::request_state::ClientRequestFailure::new("plugin_request_failed").unwrap());
            }
        }
        drop(owner);
    }
    pub(crate) fn start_plugins_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<Work>(crate::request_state::CONTROLLER_QUEUE_CAPACITY);
        let queue_observer = pioneer_observability::queues::QueueObserver::new(
            pioneer_observability::queues::QueueKind::ClientController,
            Some(crate::request_state::CONTROLLER_QUEUE_CAPACITY),
            "plugin controller requests; payload bytes unavailable without traversal",
        );
        let worker_queue = queue_observer.clone();
        let weak = Arc::downgrade(self);
        let task=std::thread::Builder::new().name("client-plugins".into()).spawn(move||while let Ok(work)=receiver.recv(){
            worker_queue.dequeued(pioneer_observability::queues::QueueBytes::Unsupported);
            let mut processing = worker_queue.processing();
            let carrier = work.work_context.clone().received(
                pioneer_observability::work::Boundary::StdThread,
                pioneer_observability::work::WorkKind::ClientCommand,
                pioneer_observability::work::ExecutorRole::ClientController,
            );
            let _scope = carrier.enter();
            let mut active = pioneer_observability::live::WorkGuard::start(carrier.context(), false);
            let Some(core)=weak.upgrade() else{return;};
            core.resume_pending_plugin_reads();
            if !core.plugin_work_current(&work){continue;}
            let current=||weak.upgrade().is_some_and(|c|c.plugin_work_current(&work));
            let mut mutation_dispatched = false;
            let mut catalog_read_succeeded = None;
            let result=(||->anyhow::Result<PluginPublication>{
                let connection=work.epoch.2.ok_or_else(||anyhow::anyhow!("gateway_not_connected"))?;
                let bound=core.transport_runtime().ws_command_sender().requests_for_connection(connection);
                let mut id=core.plugin_work_parent(&work).ok_or_else(||anyhow::anyhow!("plugin_catalog_stale"))?;
                let mut mutation_result = None;
                match &work.intent {
                    PluginIntent::SetEnabled{plugin_id,expected_revision,enabled}=>{mutation_dispatched=true;let item=crate::transport::ws::command_sender::plugins_set_enabled(&bound,PluginsSetEnabledParams{workspace_id:work.workspace.clone(),plugin_id:plugin_id.clone(),expected_revision:*expected_revision,enabled:*enabled})?;mutation_result=Some(PluginsMutationResponse{plugin:Some(item),removed:false});},
                    PluginIntent::Mutate{plugin_id,expected_revision,intent}=>{mutation_dispatched=true;mutation_result=Some(crate::transport::ws::command_sender::plugins_mutate(&bound,PluginsMutateParams{workspace_id:work.workspace.clone(),plugin_id:plugin_id.clone(),expected_revision:*expected_revision,intent:intent.clone()})?);},
                    PluginIntent::Install{upload_operation}=>{
                        anyhow::ensure!(matches!(preview_target(&core,&work.workspace,*upload_operation)?, crate::package_delivery::operations::SkillUploadTarget::PluginPreview | crate::package_delivery::operations::SkillUploadTarget::PluginInstall),"plugin_install_preview_required");
                        let preview=core.confirm_plugin_upload(&work.workspace,*upload_operation,work.epoch)?;
                        let preview=preview.plugin_preview.ok_or_else(||anyhow::anyhow!("plugin_preview_required"))?;let upload_id=preview.upload_id;let preview=preview.package;
                        mutation_dispatched=true;
                        let result = crate::transport::ws::command_sender::plugins_install(&bound,PluginsInstallParams{workspace_id:work.workspace.clone(),upload_id,expected_fingerprint:preview.fingerprint});
                        core.complete_plugin_upload_confirmation(&work.workspace,*upload_operation,work.epoch,result.is_ok());
                        let item=result?;
                        id=Some(item.id.clone());
                        mutation_result=Some(PluginsMutationResponse{plugin:Some(item),removed:false});
                    },
                    PluginIntent::ApplyUpdate{upload_operation,confirm_changes}=>{
                        let crate::package_delivery::operations::SkillUploadTarget::PluginUpdatePreview{plugin_id,expected_revision}=preview_target(&core,&work.workspace,*upload_operation)? else{anyhow::bail!("plugin_update_required");};
                        let preview=core.confirm_plugin_upload(&work.workspace,*upload_operation,work.epoch)?;
                        let preview=preview.plugin_update_preview.ok_or_else(||anyhow::anyhow!("plugin_preview_required"))?;let upload_id=preview.upload_id;let preview=preview.preview;
                        mutation_dispatched=true;
                        let result = crate::transport::ws::command_sender::plugins_mutate(&bound,PluginsMutateParams{workspace_id:work.workspace.clone(),plugin_id,expected_revision,intent:PluginManagementIntent::Update{upload_id,expected_fingerprint:preview.package.fingerprint,confirm_changes:*confirm_changes}});
                        core.complete_plugin_upload_confirmation(&work.workspace,*upload_operation,work.epoch,result.is_ok());
                        mutation_result=Some(result?);
                    },
                    _=>{},
                }
                anyhow::ensure!(current(),"plugin_catalog_stale");
                if let Some(result)=&mutation_result {
                    let _identity=core.identity_authorization.lock().expect("identity owner poisoned");
                    anyhow::ensure!(core.provider_runtime_epoch()==work.epoch,"plugin_catalog_stale");
                    let mut owner=core.plugins_controller.lock().expect("plugins poisoned");
                    anyhow::ensure!(owner.requests.get(&(work.workspace.clone(),work.key.clone())).is_some_and(|r| matches!(r.status(),crate::request_state::RequestStatus::Pending { generation,.. } if generation.get()==work.generation)),"plugin_catalog_stale");
                    if let Some(p)=owner.publications.get_mut(&work.workspace) {
                        if let Some(operation)=p.operations.get_mut(&work.key){operation.result=Some(result.clone());}
                        if let Some(item)=&result.plugin {
                            if let Some(row)=p.plugins.iter_mut().find(|row|row.id==item.id){*row=item.clone();}else{p.plugins.push(item.clone());}
                            if p.plugin_id.as_ref()!=Some(&item.id) {
                                p.skills.clear();p.skill_health.clear();p.mcp.clear();p.mcp_details.clear();p.oauth.clear();
                            }
                            p.plugin_id=Some(item.id.clone());
                            p.details=Some(item.clone());
                        } else if result.removed {
                            if let Some(id)=&id{p.plugins.retain(|row|&row.id!=id);}
                            p.details=None;
                            p.plugin_id=None;
                            id=None;
                        }
                        core.publish_plugins(&work.workspace,p);
                    }
                }
                let list=crate::transport::ws::command_sender::plugins_list(&bound,PluginsListParams{workspace_id:work.workspace.clone()});
                catalog_read_succeeded=Some(list.is_ok());
                let list=list?;
                let details=id.as_deref().and_then(|id|list.plugins.iter().find(|p|p.id==id)).cloned();
                let mut p=PluginPublication{loaded:true,connection_id:work.epoch.2,plugins:list.plugins,details,plugin_id:id,generation:work.generation,managing:core.plugins_management_allowed(&work.workspace),..Default::default()};
                core.project_plugin_native_snapshots(&work.workspace, &mut p);
                if p.details.is_some() {
                    match &work.intent {
                        PluginIntent::RemoveSkill{skill_id}=>{
                            anyhow::ensure!(p.skills.iter().any(|s|&s.skill_id==skill_id),"plugin_child_unavailable");
                            core.admit_plugin_child(&work,||core.skills_intent_bound(&work.workspace,crate::skills::operations::SkillsIntent::Remove{skill_id:skill_id.clone()},work.epoch))?;
                        },
                        PluginIntent::ConfigureMcp{server_id,body}=>{
                            let server=p.mcp.iter().find(|s|&s.id==server_id).ok_or_else(||anyhow::anyhow!("plugin_child_unavailable"))?;
                            let config_json=crate::mcp::actions::owned_server_config_for_submit(body,&server.name).ok_or_else(||anyhow::anyhow!("mcp_config_invalid"))?;
                            let native_id = core.admit_plugin_child(&work,||core.mcp_intent_bound(&work.workspace,crate::mcp::operations::McpIntent::Configure{config_json},work.epoch))?;
                            // Link only the ID returned by the existing native owner.
                            // Native completion may race this projection; lookup is exact.
                            let _identity = core.identity_authorization.lock().expect("identity owner poisoned");
                            let mut owner = core.plugins_controller.lock().expect("plugins poisoned");
                            if core.provider_runtime_epoch() == work.epoch {
                                if let Some(publication) = owner.publications.get_mut(&work.workspace) {
                                    if let Some(operation) = publication.operations.get_mut(&work.key).filter(|o| o.generation == work.generation) {
                                        operation.native_operation_id = Some(native_id);
                                    }
                                    core.publish_plugins(&work.workspace, publication);
                                }
                            }
                        },
                        PluginIntent::Skill{skill_id,enabled,allow_implicit_invocation}=>{
                            anyhow::ensure!(p.skills.iter().any(|s|&s.skill_id==skill_id),"plugin_child_unavailable");
                            core.admit_plugin_child(&work,||core.skills_intent_bound(&work.workspace,crate::skills::operations::SkillsIntent::Policy{skill_id:skill_id.clone(),enabled:*enabled,allow_implicit_invocation:*allow_implicit_invocation},work.epoch))?;
                        },
                        PluginIntent::Mcp{intent}=>{
                            use crate::mcp::operations::McpIntent::*;
                            let server_id=match intent{SignIn{server_id}|Disconnect{server_id}|CancelAuthorization{server_id,..}|RetryAuthorizationBrowser{server_id}|Policy{server_id,..}|Restart{server_id}|Remove{server_id}=>server_id,Configure{..}=>anyhow::bail!("plugin_child_configure_forbidden")};
                            anyhow::ensure!(p.mcp.iter().any(|s|&s.id==server_id),"plugin_child_unavailable");
                            core.admit_plugin_child(&work,||core.mcp_intent_bound(&work.workspace,intent.clone(),work.epoch))?;
                        },_=>{}
                    }
                } else { anyhow::ensure!(!matches!(work.intent,PluginIntent::Skill{..}|PluginIntent::Mcp{..}|PluginIntent::RemoveSkill{..}|PluginIntent::ConfigureMcp{..}),"plugin_child_unavailable"); }
                Ok(p)
            })();
            let outcome = if result.is_ok() { pioneer_observability::work::Outcome::Completed } else { pioneer_observability::work::Outcome::Failed };
            core.settle_plugin_work(&work,result,mutation_dispatched,catalog_read_succeeded);
            core.resume_pending_plugin_reads();
            processing.processed();
            active.finish(outcome);
        }).expect("plugins worker start");
        let mut owner = self.plugins_controller.lock().expect("plugins poisoned");
        owner.observer = Some(queue_observer);
        owner.sender = Some(sender);
        owner.task = Some(task);
        drop(owner);
        self.resume_pending_plugin_reads();
    }
}
fn preview_target(
    core: &ClientCore,
    workspace: &str,
    id: u64,
) -> anyhow::Result<crate::package_delivery::operations::SkillUploadTarget> {
    core.skill_upload_snapshot(workspace, id)
        .map(|p| p.target.clone())
        .ok_or_else(|| anyhow::anyhow!("plugin_preview_required"))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::core::ClientSubscriptionEvent;
    // Shared test fixture only: exercise real native completion owners against
    // a live Plugins consumer without exposing private work or production hooks.
    pub(crate) fn watch_plugin_reads(core: &Arc<ClientCore>) -> (PluginDemand, impl Fn()) {
        let mut ready = PluginPublication {
            loaded: true, connection_id: core.plugin_command_origin().connection_id,
            plugins: vec![], ..Default::default()
        };
        core.publish_plugins("workspace", &mut ready);
        let (sender, receiver) = mpsc::sync_channel(4);
        {
            let mut owner = core.plugins_controller.lock().unwrap();
            owner.publications.insert("workspace".into(), ready);
            owner.sender = Some(sender);
        }
        let demand = core.acquire_plugin_demand("workspace", None);
        let core = core.clone();
        (demand, move || {
            core.resume_pending_plugin_reads();
            assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)), "standalone completion requested a Plugins read");
        })
    }
    #[test]
    fn precommit_resume_cannot_resurrect_parent_results_after_eviction() {
        let core = crate::catalog_test_support::client();
        let (sender, requests) = mpsc::sync_channel(8);
        core.plugins_controller.lock().unwrap().sender = Some(sender);
        let _lease = core.acquire_plugin_demand("workspace", None);
        let before = requests.try_recv().unwrap();
        assert!(core.plugin_work_current(&before));
        let old_epoch = before.epoch;
        let old_generation = before.generation;
        let scope = ClientScope::Plugins { workspace_id: "workspace".into() };
        let subscription = core.subscribe(scope.clone(), std::num::NonZeroUsize::new(64).unwrap());
        let original = (*core.snapshot(&ClientScope::Administration { workspace_id: None }).unwrap()
            .snapshot().payload::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>().unwrap()).clone();
        let mut denied = original.clone();
        denied.authorization_change_sequence += 1;
        denied.capabilities.clear_epoch();
        let (paused, pause) = mpsc::channel();
        let (commit, committed) = mpsc::channel();
        let publisher_core = core.clone();
        let publisher = std::thread::spawn(move || {
            crate::core::IDENTITY_BEFORE_COMMIT.with_borrow_mut(|hook| {
                *hook = Some(Box::new(move || { paused.send(()).unwrap(); committed.recv_timeout(std::time::Duration::from_secs(5)).unwrap(); }));
            });
            let _identity = publisher_core.identity_authorization.lock().unwrap();
            publisher_core.publish_identity_authorization(&denied,
                crate::gateway::identity_authorization::IdentityPublicationChange::Revalidate);
        });
        pause.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        let (attempted, attempt) = mpsc::channel();
        let (claimed, claim) = mpsc::channel();
        let (respond, response_ready) = mpsc::channel();
        let reader_core = core.clone();
        let reader = std::thread::spawn(move || {
            attempted.send(()).unwrap();
            reader_core.resume_plugin_publications();
            reader_core.plugin_intent("workspace", PluginIntent::Observe { plugin_id: None });
            let next = requests.try_recv().ok();
            claimed.send(next.as_ref().map(|work| (work.epoch, work.generation))).unwrap();
            response_ready.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
            let result = |work: &Work| Ok(PluginPublication {
                loaded: true, connection_id: work.epoch.2, generation: work.generation,
                plugins: vec![PluginItem { id: "P".repeat(21), name: "response".into(), version: None,
                    description: None, enabled: true, state: "installed".into(), revision: 1,
                    status: "installed".into(), components: vec![], diagnostics: vec![] }],
                ..Default::default()
            });
            reader_core.settle_plugin_work(&before, result(&before), false, Some(true));
            if let Some(work) = next { reader_core.settle_plugin_work(&work, result(&work), false, Some(true)); }
            requests
        });
        attempt.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        if let Ok(Some((epoch, generation))) = claim.recv_timeout(std::time::Duration::from_millis(100)) {
            assert_eq!(epoch, old_epoch);
            assert_ne!(generation, old_generation);
        }
        commit.send(()).unwrap();
        publisher.join().unwrap();
        while subscription.try_next().is_some() {}
        respond.send(()).unwrap();
        let requests = reader.join().unwrap();
        let current = || core.snapshot(&scope).and_then(|p| p.snapshot().payload::<PluginPublication>());
        assert!(current().is_none_or(|p| p.plugins.is_empty()));
        while let Some(event) = subscription.try_next() {
            if let ClientSubscriptionEvent::Publication { publication, .. } = event {
                if let Some(value) = publication.snapshot().payload::<PluginPublication>() { assert!(value.plugins.is_empty()); }
            }
        }
        let mut granted = original;
        granted.authorization_change_sequence += 2;
        {
            let _identity = core.identity_authorization.lock().unwrap();
            core.publish_identity_authorization(&granted,
                crate::gateway::identity_authorization::IdentityPublicationChange::Revalidate);
        }
        core.resume_plugin_publications();
        let fresh = requests.try_recv().expect("current regrant must produce a fresh parent Observe");
        assert_ne!(fresh.epoch, old_epoch);
        core.settle_plugin_work(&fresh, Ok(PluginPublication {
            loaded: true, connection_id: fresh.epoch.2, generation: fresh.generation,
            plugins: vec![PluginItem { id: "F".repeat(21), name: "fresh".into(), version: None,
                description: None, enabled: true, state: "installed".into(), revision: 1,
                status: "installed".into(), components: vec![], diagnostics: vec![] }],
            ..Default::default()
        }), false, Some(true));
        assert_eq!(current().unwrap().plugins[0].name, "fresh");
    }
    // NOT_RUN / NOT_COMPILED. No transport/app/fixture is started.
    #[test]
    fn closed_surface_and_uncertain_projection_cannot_admit_mutations() {
        let core = crate::catalog_test_support::client();
        let params = PluginIntent::SetEnabled {
            plugin_id: "P".repeat(21),
            expected_revision: 7,
            enabled: false,
        };
        assert_eq!(
            core.plugin_intent("workspace", params.clone()).outcome(),
            crate::core::ClientTransitionOutcome::Rejected
        );
        core.plugins_controller.lock().unwrap().publications.insert(
            "workspace".into(),
            PluginPublication {
                error: Some("uncertain".into()),
                ..Default::default()
            },
        );
        assert_eq!(
            core.plugin_intent("workspace", params.clone()).outcome(),
            crate::core::ClientTransitionOutcome::Rejected
        );
        core.plugin_intent("workspace", PluginIntent::Close);
        assert_eq!(
            core.plugin_intent("workspace", params).outcome(),
            crate::core::ClientTransitionOutcome::Rejected
        );
    }
    #[test]
    fn close_then_reopen_cannot_revive_an_old_read_with_the_same_counter() {
        let core = crate::catalog_test_support::client();
        let (sender, receiver) = mpsc::sync_channel(4);
        core.plugins_controller.lock().unwrap().sender = Some(sender);
        core.plugin_intent("workspace", PluginIntent::Observe { plugin_id: None });
        let old = receiver.try_recv().unwrap();
        core.plugin_intent("workspace", PluginIntent::Close);
        core.plugin_intent(
            "workspace",
            PluginIntent::Observe {
                plugin_id: Some("P".repeat(21)),
            },
        );
        let new = receiver.try_recv().unwrap();
        assert_ne!(old.generation, new.generation);
        assert!(!core.plugin_work_current(&old));
        assert!(core.plugin_work_current(&new));
    }
    // NOT_RUN / NOT_COMPILED. Exercise the worker's actual pre-RPC boundary,
    // with a deterministic pause after its first check. No RPC/fixture is run.
    #[test]
    fn paused_worker_discards_close_stop_and_reopen_then_handles_next_work() {
        for (stop, reopen_before_resume) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let core = crate::catalog_test_support::client();
            let (sender, receiver) = mpsc::sync_channel(4);
            let (discarded_tx, discarded_rx) = mpsc::channel();
            core.plugins_controller.lock().unwrap().sender = Some(sender.clone());
            core.plugin_intent(
                "workspace",
                PluginIntent::Observe {
                    plugin_id: Some("old".into()),
                },
            );
            let old = receiver.recv().unwrap();
            let pause = Arc::new(std::sync::Barrier::new(2));
            let resume = Arc::new(std::sync::Barrier::new(2));
            let worker_core = core.clone();
            let worker_pause = pause.clone();
            let worker_resume = resume.clone();
            let worker = std::thread::spawn(move || {
                assert!(worker_core.plugin_work_current(&old));
                worker_pause.wait();
                worker_resume.wait();
                assert_eq!(old.plugin_id.as_deref(), Some("old"));
                assert!(worker_core.plugin_work_parent(&old).is_none());
                assert!(
                    worker_core
                        .admit_plugin_child(&old, || panic!("stale child admitted"))
                        .is_err()
                );
                discarded_tx.send(()).unwrap();
                let next = receiver.recv().unwrap();
                assert_eq!(
                    worker_core.plugin_work_parent(&next),
                    Some(Some("new".into()))
                );
            });
            pause.wait();
            if stop {
                core.plugins_controller.lock().unwrap().stop();
            } else {
                core.plugin_intent("workspace", PluginIntent::Close);
            }
            assert!(
                core.plugins_controller
                    .lock()
                    .unwrap()
                    .publications
                    .is_empty()
            );
            core.plugins_controller.lock().unwrap().sender = Some(sender);
            let reopen = || {
                core.plugin_intent(
                    "workspace",
                    PluginIntent::Observe {
                        plugin_id: Some("new".into()),
                    },
                )
            };
            if reopen_before_resume {
                reopen();
            }
            resume.wait();
            discarded_rx.recv().unwrap();
            if !reopen_before_resume {
                reopen();
            }
            worker.join().unwrap();
            assert!(core.plugins_controller.lock().is_ok());
        }
    }
    #[test]
    fn foreign_connection_cannot_close_or_change_current_projection() {
        let core = crate::catalog_test_support::client();
        core.plugins_controller.lock().unwrap().publications.insert(
            "workspace".into(),
            PluginPublication {
                generation: 17,
                ..Default::default()
            },
        );
        let wrong = core.current_auth_ticket().1.map(|id| id + 1).or(Some(99));
        let result = core.dispatch(crate::core::ClientIntent::Plugins {
            workspace_id: "workspace".into(),
            connection_id: wrong,
            intent: PluginIntent::Close,
        });
        assert_eq!(
            result.outcome(),
            crate::core::ClientTransitionOutcome::Rejected
        );
        assert_eq!(
            core.plugins_controller.lock().unwrap().publications["workspace"].generation,
            17
        );
    }

    #[test]
    fn warm_catalog_and_details_survive_focus_and_revalidate_only_after_invalidation() {
        let core = crate::catalog_test_support::client();
        let plugin = PluginItem {
            id: "P".repeat(21),
            name: "Plugin".into(),
            version: None,
            description: None,
            enabled: true,
            state: "installed".into(),
            revision: 3,
            status: "installed".into(),
            components: vec![],
            diagnostics: vec![],
        };
        let mut ready = PluginPublication {
            loaded: true,
            connection_id: core.provider_runtime_epoch().2,
            plugin_id: Some(plugin.id.clone()),
            plugins: vec![plugin.clone()],
            details: Some(plugin.clone()),
            ..Default::default()
        };
        ready.plugins[0].components.push(PluginComponentItem {
            kind: "mcp".into(), member_key: "member".into(), status: "installed".into(),
            diagnostic: None, skill_id: None, mcp_installation_id: Some("member".into()), runtime_status: None,
        });
        ready.details = Some(ready.plugins[0].clone());
        core.publish_plugins("workspace", &mut ready);
        let (sender, requests) = mpsc::sync_channel(4);
        {
            let mut owner = core.plugins_controller.lock().unwrap();
            owner.sender = Some(sender);
            owner.publications.insert("workspace".into(), ready.clone());
        }
        let first = core.acquire_plugin_demand("workspace", Some(&plugin.id));
        let second = core.acquire_plugin_demand("workspace", Some(&plugin.id));
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        drop(first);
        assert!(
            !core
                .plugins_controller
                .lock()
                .unwrap()
                .suspended
                .contains("workspace")
        );
        drop(second);
        let publication = core
            .snapshot(&ClientScope::Plugins {
                workspace_id: "workspace".into(),
            })
            .unwrap()
            .snapshot()
            .payload::<PluginPublication>()
            .unwrap();
        assert_eq!(*publication, ready);
        core.refresh_plugin_publication("other-workspace");
        core.refresh_plugin_publication("workspace");
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        let resumed = core.acquire_plugin_demand("workspace", Some(&plugin.id));
        let request = requests.try_recv().unwrap();
        assert_eq!(request.plugin_id.as_deref(), Some(plugin.id.as_str()));
        let publication = core
            .snapshot(&ClientScope::Plugins {
                workspace_id: "workspace".into(),
            })
            .unwrap()
            .snapshot()
            .payload::<PluginPublication>()
            .unwrap();
        assert!(publication.loaded && publication.busy);
        assert_eq!(publication.plugins, ready.plugins);
        assert_eq!(publication.details, ready.details);
        let same_request = core.acquire_plugin_demand("workspace", Some(&plugin.id));
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        drop(same_request);
        drop(resumed);
        assert!(core.plugin_work_current(&request));
        let native_event = |server: &str| GatewayNotification::McpOAuthChanged(McpOAuthNotification {
            workspace_id: "workspace".into(), server_id: server.into(), name: server.into(),
            scope_kind: McpScopeKind::Workspace, flow_id: None, state: McpOAuthState::Authorized,
            authorization_url: None, diagnostic: None,
        });
        core.refresh_plugins_for_native_notification(&native_event("standalone"));
        assert!(!core.plugins_controller.lock().unwrap().refresh_requested.contains("workspace"));
        core.refresh_plugins_for_native_notification(&native_event("member"));
        assert!(core.plugins_controller.lock().unwrap().refresh_requested.contains("workspace"));
        {
            let mut owner = core.plugins_controller.lock().unwrap();
            owner.publications.get_mut("workspace").unwrap().settle_operation(&request.key, Ok(ready.clone()), false, Some(true), &Default::default());
            owner.requests.get_mut(&("workspace".into(), request.key.clone())).unwrap().succeed(crate::core::ClientGeneration::new(request.generation), ());
        }
        let owned_demand = core.acquire_plugin_demand("workspace", Some(&plugin.id));
        let owned_read = requests.try_recv().unwrap();
        assert!(matches!(owned_read.intent, PluginIntent::Observe { .. }));
        assert_eq!(owned_read.plugin_id, Some(plugin.id.clone()));
        assert!(core.plugin_work_current(&owned_read));
        drop(owned_demand);
        // The receiver's capacity-release continuation is shared across scopes:
        // X has real active/queued commands while W needs its first or fresh read.
        for initial in [false, true] {
            let core = crate::catalog_test_support::client();
            let other = PluginItem { id: "B".repeat(21), ..plugin.clone() };
            let mut inventory = ready.clone();
            inventory.plugins.push(other.clone());
            let (sender, receiver) = mpsc::sync_channel(1);
            {
                let mut owner = core.plugins_controller.lock().unwrap();
                owner.sender = Some(sender);
                owner.publications.insert("X".into(), inventory.clone());
                if !initial { owner.publications.insert("W".into(), inventory.clone()); }
            }
            let origin = core.plugin_command_origin();
            for (index, parent) in [&plugin, &other].into_iter().enumerate() {
                core.submit_plugin_command("X", &parent.id, PluginIntent::SetEnabled {
                    plugin_id: parent.id.clone(), expected_revision: parent.revision, enabled: false,
                }, &origin).unwrap();
                if index == 0 {
                    let active = receiver.try_recv().unwrap();
                    assert!(core.plugin_work_current(&active)); // X RPC is in flight.
                }
            }
            let demand = core.acquire_plugin_demand("W", Some(&plugin.id));
            if !initial { core.refresh_plugin_publication("W"); }
            for _ in 0..2 {
                assert_eq!(core.plugin_intent("W", PluginIntent::Observe { plugin_id: Some(other.id.clone()) }).outcome(), crate::core::ClientTransitionOutcome::Rejected);
                core.resume_pending_plugin_reads(); // Still Full, never drop the obligation.
            }
            {
                let owner = core.plugins_controller.lock().unwrap();
                assert_eq!(owner.pending_observations.len(), 1);
                assert_eq!(owner.pending_observations["W"], (Some(other.id.clone()), origin.clone()));
                assert!(!owner.publications.get("W").is_some_and(|p| p.busy));
            }
            let queued_x = receiver.try_recv().unwrap();
            assert_eq!(queued_x.workspace, "X");
            assert!(core.plugin_work_current(&queued_x));
            // This is the actual production boundary invoked after dequeue and
            // settlement, without a further notification/demand event from W.
            core.resume_pending_plugin_reads();
            core.resume_pending_plugin_reads();
            let read = receiver.try_recv().unwrap();
            assert_eq!(read.workspace, "W");
            assert_eq!(read.plugin_id, Some(other.id.clone()));
            assert_eq!(read.epoch, origin.epoch());
            assert!(matches!(read.intent, PluginIntent::Observe { .. }));
            assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
            let complete_read = |work: &Work| {
                assert!(core.plugin_work_current(work));
                let mut owner = core.plugins_controller.lock().unwrap();
                let publication = owner.publications.get_mut("W").unwrap();
                let mut observed = inventory.clone();
                observed.plugin_id = work.plugin_id.clone();
                observed.plugins[1].revision += 1;
                observed.connection_id = work.epoch.2;
                publication.settle_operation(&work.key, Ok(observed), false, Some(true), &Default::default());
                owner.reconcile_recovery_reads("W");
                owner.requests.get_mut(&("W".into(), work.key.clone())).unwrap().succeed(crate::core::ClientGeneration::new(work.generation), ());
                core.publish_plugins("W", owner.publications.get_mut("W").unwrap());
            };
            complete_read(&read);
            core.resume_pending_plugin_reads();
            assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
            assert_eq!(core.plugin_details_publication("W", &other.id).unwrap().details.unwrap().revision, other.revision + 1);
            // A newer explicitly admitted selection supersedes an older Full
            // obligation; its completion must not revive the old selection.
            core.plugin_intent("selection-blocker", PluginIntent::Observe { plugin_id: None });
            core.plugin_intent("W", PluginIntent::Observe { plugin_id: Some(other.id.clone()) });
            assert_eq!(receiver.try_recv().unwrap().workspace, "selection-blocker");
            core.plugin_intent("W", PluginIntent::Observe { plugin_id: Some(plugin.id.clone()) });
            let selected = receiver.try_recv().unwrap();
            assert_eq!(selected.plugin_id, Some(plugin.id.clone()));
            complete_read(&selected);
            core.resume_pending_plugin_reads();
            assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
            if initial { continue; } // Lifecycle boundaries need one owner proof.

            // A released demand suspends a retained obligation until reacquire.
            core.refresh_plugin_publication("W");
            let in_flight = receiver.try_recv().unwrap();
            core.refresh_plugin_publication("W");
            drop(demand);
            complete_read(&in_flight);
            core.resume_pending_plugin_reads();
            assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
            let resumed = core.acquire_plugin_demand("W", Some(&other.id));
            let resumed_read = receiver.try_recv().unwrap();
            complete_read(&resumed_read);
            drop(resumed);
            core.plugin_intent("W", PluginIntent::Close);
            assert_eq!(core.plugin_intent_bound("W", PluginIntent::Observe { plugin_id: Some(other.id.clone()) }, false, None, Some(&origin), true).outcome(), crate::core::ClientTransitionOutcome::Rejected);
            core.resume_pending_plugin_reads();
            assert!(!core.plugins_controller.lock().unwrap().publications.contains_key("W"));
            assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));

            // Initial demand with no publication also survives auth invalidation;
            // old captured selection/origin cannot authorize its continuation.
            core.plugin_intent("auth-blocker", PluginIntent::Observe { plugin_id: None });
            let resumed = core.acquire_plugin_demand("W", Some(&other.id));
            assert!(!core.plugins_controller.lock().unwrap().publications.contains_key("W"));
            let old_read = receiver.try_recv().unwrap();
            core.invalidate_plugin_publications();
            assert!(core.plugins_controller.lock().unwrap().pending_observations.is_empty());
            crate::catalog_test_support::replace_connection(&core, 8);
            assert!(!core.plugin_work_current(&old_read));
            assert_eq!(core.plugin_intent_bound("W", PluginIntent::Observe { plugin_id: Some(plugin.id.clone()) }, false, None, Some(&origin), true).outcome(), crate::core::ClientTransitionOutcome::Rejected);
            core.resume_pending_plugin_reads();
            assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
            core.resume_plugin_publications();
            let mut fresh_w = None;
            for _ in 0..4 { // Each dequeue releases capacity for one retained scope.
                let work = receiver.try_recv().unwrap();
                if work.workspace == "W" { fresh_w = Some(work); break; }
                core.resume_pending_plugin_reads();
            }
            let fresh_w = fresh_w.unwrap();
            assert_eq!(fresh_w.epoch, core.plugin_command_origin().epoch());
            assert!(fresh_w.plugin_id.is_none(), "old authorization selection was discarded");
            core.plugins_controller.lock().unwrap().stop();
            core.resume_pending_plugin_reads();
            assert!(!core.plugin_work_current(&fresh_w));
            assert!(core.plugins_controller.lock().unwrap().pending_observations.is_empty());
            assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Disconnected)));
            drop(resumed);
        }
    }
    #[test]
    fn two_parent_surfaces_keep_their_details_and_outcomes_through_queued_settlement() {
        let core = crate::catalog_test_support::client();
        let parent = |letter: char| PluginItem {
            id: letter.to_string().repeat(21),
            name: letter.to_string(),
            version: None,
            description: None,
            enabled: true,
            state: "installed".into(),
            revision: 7,
            status: "installed".into(),
            components: vec![],
            diagnostics: vec![],
        };
        let a = parent('A');
        let b = parent('B');
        let mut ready = PluginPublication {
            loaded: true,
            connection_id: core.provider_runtime_epoch().2,
            plugin_id: Some(a.id.clone()),
            details: Some(a.clone()),
            plugins: vec![a.clone(), b.clone()],
            ..Default::default()
        };
        core.publish_plugins("workspace", &mut ready);
        let (sender, receiver) = mpsc::sync_channel(4);
        {
            let mut owner = core.plugins_controller.lock().unwrap();
            owner.sender = Some(sender);
            owner.publications.insert("workspace".into(), ready);
        }
        let first = core.acquire_plugin_demand("workspace", Some(&a.id));
        let second = core.acquire_plugin_demand("workspace", Some(&b.id));
        assert!(receiver.try_recv().is_err());
        let origin = core.plugin_command_origin();
        let mut foreign = origin.clone();
        foreign.authorization_sequence += 1;
        assert!(
            core.submit_plugin_command(
                "workspace",
                &a.id,
                PluginIntent::SetEnabled {
                    plugin_id: a.id.clone(),
                    expected_revision: 7,
                    enabled: false,
                },
                &foreign
            )
            .is_err()
        );
        assert!(receiver.try_recv().is_err());
        // Rejected admission preserves current outcomes, request generations and
        // read locators, and leaves the queue empty. Shared/FFI uses this entry.
        let rejected_without_replacement = |intent| {
            let (before, reads, requests) = {
                let owner = core.plugins_controller.lock().unwrap();
                (owner.publications["workspace"].clone(), owner.recovery_reads.clone(), owner.requests.clone())
            };
            assert!(core.submit_plugin_command("workspace", &a.id, intent, &core.plugin_command_origin()).is_err());
            assert!(matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
            let owner = core.plugins_controller.lock().unwrap();
            assert_eq!(owner.publications["workspace"], before);
            assert_eq!(owner.recovery_reads, reads);
            assert_eq!(owner.requests, requests);
        };
        // Normal valid A@7, managing origin and a live empty sender: a raw
        // Update must not become admitted work without an upload recovery owner.
        rejected_without_replacement(PluginIntent::Mutate {
            plugin_id: a.id.clone(), expected_revision: 7,
            intent: PluginManagementIntent::Update { upload_id: "raw-upload".into(), expected_fingerprint: "raw-fingerprint".into(), confirm_changes: true },
        });
        let receipts = [&a, &b].map(|parent| {
            core.submit_plugin_command(
                "workspace",
                &parent.id,
                PluginIntent::SetEnabled {
                    plugin_id: parent.id.clone(),
                    expected_revision: 7,
                    enabled: false,
                },
                &origin,
            )
            .unwrap()
        });
        for receipt in &receipts {
            let operation = core.plugin_command_operation("workspace", receipt).unwrap();
            assert!(operation.busy && operation.native_operation_id.is_none());
            assert_eq!(operation.generation, receipt.generation);
        }
        let first_work = receiver.try_recv().unwrap();
        let second_work = receiver.try_recv().unwrap();
        assert_eq!(
            core.plugin_work_parent(&first_work),
            Some(Some(a.id.clone()))
        );
        assert_eq!(
            core.plugin_work_parent(&second_work),
            Some(Some(b.id.clone()))
        );
        core.plugin_intent("workspace", PluginIntent::Close);
        drop(first);
        let a_view = core.plugin_details_publication("workspace", &a.id).unwrap();
        let b_view = core.plugin_details_publication("workspace", &b.id).unwrap();
        assert_eq!(a_view.details, Some(a.clone()));
        assert_eq!(b_view.details, Some(b.clone()));
        assert!(a_view.busy && b_view.busy);
        assert!(core.plugin_work_current(&first_work));
        assert!(core.plugin_work_current(&second_work));
        // These are inputs at the worker's settlement boundary: a typed
        // response has already been recorded before its follow-up list read.
        // Exercise that production reducer and the real scoped surface getters;
        // no transport/persistence outcome is claimed by this fixture.
        let pending = core.plugins_controller.lock().unwrap().publications["workspace"].clone();
        let retained = [a.id.clone(), b.id.clone()].into_iter().collect();
        let a_result = PluginsMutationResponse {
            plugin: Some(a.clone()),
            removed: false,
        };
        let b_result = PluginsMutationResponse {
            plugin: Some(b.clone()),
            removed: false,
        };
        let removed = PluginsMutationResponse {
            plugin: None,
            removed: true,
        };
        for (label, response, succeeded, dispatched, read, prior_read_error) in [
            ("lost mutation response", None, false, true, None, None),
            ("native success", None, true, false, Some(true), None),
            (
                "native admission failure",
                None,
                false,
                false,
                Some(true),
                None,
            ),
            (
                "typed success",
                Some(b_result.clone()),
                true,
                true,
                Some(true),
                None,
            ),
            (
                "typed success then read failure",
                Some(b_result),
                false,
                true,
                Some(false),
                None,
            ),
            (
                "remove success then read failure",
                Some(removed),
                false,
                true,
                Some(false),
                None,
            ),
            (
                "command failure preserves read error",
                None,
                false,
                false,
                None,
                Some("earlier catalog read"),
            ),
        ] {
            let mut publication = pending.clone();
            publication.operations.get_mut(&a.id).unwrap().result = Some(a_result.clone());
            let next = publication.clone();
            publication.settle_operation(&first_work.key, Ok(next), true, Some(true), &retained);
            assert!(publication.busy, "B remains queued: {label}");
            publication.error = prior_read_error.map(str::to_owned);
            publication.operations.get_mut(&b.id).unwrap().result = response.clone();
            let result = if succeeded {
                Ok(publication.clone())
            } else {
                Err(anyhow::anyhow!("B failed"))
            };
            publication.settle_operation(&second_work.key, result, dispatched, read, &retained);
            assert!(!publication.busy, "{label}");
            core.publish_plugins("workspace", &mut publication);
            let a_view = core.plugin_details_publication("workspace", &a.id).unwrap();
            let b_view = core.plugin_details_publication("workspace", &b.id).unwrap();
            assert_eq!(a_view.operations[&a.id].result, Some(a_result.clone()), "{label}");
            assert!(!a_view.busy && !a_view.mutation_uncertain, "{label}");
            let shared_error = if read == Some(false) {
                Some("B failed")
            } else {
                prior_read_error
            };
            assert_eq!(a_view.error.as_deref(), shared_error, "{label}");
            assert_eq!(b_view.operations[&b.id].result, response, "{label}");
            assert_eq!(
                core.plugin_command_operation("workspace", &receipts[0])
                    .unwrap()
                    .result,
                Some(a_result.clone()),
                "{label}"
            );
            assert_eq!(
                core.plugin_command_operation("workspace", &receipts[1])
                    .unwrap()
                    .result,
                response,
                "{label}"
            );
            assert_eq!(
                b_view.mutation_uncertain,
                dispatched && !succeeded && response.is_none(),
                "{label}"
            );
            assert_eq!(
                b_view.error.as_deref(),
                (!succeeded).then_some("B failed"),
                "{label}"
            );
        }
        // Upload work legitimately aliases its own authoritative parent, even
        // if its subsequent list read fails. It must not overwrite a busy key.
        for parent_busy in [false, true] {
            let mut publication = pending.clone();
            publication.operations.get_mut(&a.id).unwrap().busy = parent_busy;
            publication.operations.insert(
                "upload:42".into(),
                PluginOperationPublication {
                    busy: true,
                    result: Some(a_result.clone()),
                    ..Default::default()
                },
            );
            publication.settle_operation(
                "upload:42",
                Err(anyhow::anyhow!("catalog read failed")),
                true,
                Some(false),
                &retained,
            );
            assert_eq!(publication.operations[&a.id].busy, parent_busy);
            assert_eq!(
                publication.operations[&a.id].result,
                (!parent_busy).then_some(a_result.clone())
            );
            assert!(!publication.operations["upload:42"].uncertain);
        }
        // Ordinary lost response: use the actual admitted operation, parent,
        // authority and available sender, then the worker's settlement reducer.
        {
            let mut owner = core.plugins_controller.lock().unwrap();
            let publication = owner.publications.get_mut("workspace").unwrap();
            publication.settle_operation(&first_work.key, Err(anyhow::anyhow!("lost response")), true, None, &retained);
            let mut next = publication.clone();
            next.operations.get_mut(&b.id).unwrap().result = Some(PluginsMutationResponse { plugin: Some(b.clone()), removed: false });
            publication.operations.get_mut(&b.id).unwrap().result = next.operations[&b.id].result.clone();
            publication.settle_operation(&second_work.key, Ok(next), true, Some(true), &retained);
            owner.requests.get_mut(&("workspace".into(), a.id.clone())).unwrap().fail(
                crate::core::ClientGeneration::new(first_work.generation),
                crate::request_state::ClientRequestFailure::new("lost_response").unwrap(),
            );
            owner.requests.get_mut(&("workspace".into(), b.id.clone())).unwrap().succeed(
                crate::core::ClientGeneration::new(second_work.generation), (),
            );
        }
        let set_enabled = |revision| PluginIntent::SetEnabled {
            plugin_id: a.id.clone(), expected_revision: revision, enabled: false,
        };
        assert!(!core.plugin_parent_recovery_available("workspace", &a.id));
        rejected_without_replacement(set_enabled(7));
        rejected_without_replacement(PluginIntent::Mutate {
            plugin_id: a.id.clone(), expected_revision: 7, intent: PluginManagementIntent::Continue,
        });
        rejected_without_replacement(PluginIntent::Mutate {
            plugin_id: a.id.clone(), expected_revision: 7,
            intent: PluginManagementIntent::Update { upload_id: "upload".into(), expected_fingerprint: "fingerprint".into(), confirm_changes: true },
        });
        // Successful/failed read inputs at the real Observe enqueue/reducer
        // boundary. No RPC completion or upload-engine execution is fabricated.
        let observe = |revision: Option<i64>| {
            let transition = core.plugin_intent("workspace", PluginIntent::Observe { plugin_id: Some(a.id.clone()) });
            assert_ne!(transition.outcome(), crate::core::ClientTransitionOutcome::Rejected);
            let work = receiver.try_recv().unwrap();
            assert!(matches!(work.intent, PluginIntent::Observe { .. }));
            let cached_revision = core.plugins_controller.lock().unwrap().publications["workspace"].plugins.iter().find(|item| item.id == a.id).unwrap().revision;
            rejected_without_replacement(set_enabled(cached_revision));
            let mut owner = core.plugins_controller.lock().unwrap();
            let publication = owner.publications.get_mut("workspace").unwrap();
            let result = match revision {
                Some(revision) => {
                    let mut observed = publication.clone();
                    observed.plugins.iter_mut().find(|item| item.id == a.id).unwrap().revision = revision;
                    Ok(observed)
                }
                None => Err(anyhow::anyhow!("read failed")),
            };
            publication.settle_operation(&work.key, result, false, Some(revision.is_some()), &retained);
            if revision.is_some() { owner.reconcile_recovery_reads("workspace"); }
            let request = owner.requests.get_mut(&("workspace".into(), work.key)).unwrap();
            let generation = crate::core::ClientGeneration::new(work.generation);
            if revision.is_some() { request.succeed(generation, ()); }
            else { request.fail(generation, crate::request_state::ClientRequestFailure::new("read_failed").unwrap()); }
            let publication = owner.publications.get_mut("workspace").unwrap();
            assert!(publication.operations[&a.id].uncertain);
            assert!(publication.operations[&a.id].result.is_none());
            core.publish_plugins("workspace", publication);
        };
        observe(Some(8));
        assert!(core.plugin_parent_recovery_available("workspace", &a.id));
        assert!(!core.plugin_parent_recovery_available("workspace", &b.id));
        rejected_without_replacement(set_enabled(7)); // Stale CAS.
        let read_identity = {
            let mut owner = core.plugins_controller.lock().unwrap();
            let evidence = owner.recovery_reads.get_mut(&("workspace".into(), a.id.clone())).unwrap();
            let saved = *evidence;
            evidence.0 += 1;
            saved
        };
        rejected_without_replacement(set_enabled(8)); // Evidence for another operation generation.
        core.plugins_controller.lock().unwrap().recovery_reads.insert(("workspace".into(), a.id.clone()), read_identity);
        // A later inventory revision cannot reuse an older recovery read.
        {
            let mut owner = core.plugins_controller.lock().unwrap();
            owner.publications.get_mut("workspace").unwrap().plugins.iter_mut().find(|item| item.id == a.id).unwrap().revision = 9;
        }
        rejected_without_replacement(set_enabled(9));
        observe(None);
        rejected_without_replacement(set_enabled(9)); // Failed read.
        observe(Some(8));
        crate::catalog_test_support::replace_connection(&core, 8);
        rejected_without_replacement(set_enabled(8)); // Current caller, old evidence origin.
        crate::catalog_test_support::replace_connection(&core, 7);
        // Fill the bounded queue with actual admitted B work, then attempt an
        // otherwise eligible A recovery. No worker/transport timing is needed.
        let (bounded_sender, bounded_receiver) = mpsc::sync_channel(1);
        let previous_sender = core.plugins_controller.lock().unwrap().sender.replace(bounded_sender).unwrap();
        let blocker = core.submit_plugin_command("workspace", &b.id, PluginIntent::SetEnabled {
            plugin_id: b.id.clone(), expected_revision: 7, enabled: false,
        }, &origin).unwrap();
        assert!(core.plugin_parent_recovery_available("workspace", &a.id));
        let prior_generation = core.plugins_controller.lock().unwrap().requests[&("workspace".into(), a.id.clone())].latest_generation().get();
        rejected_without_replacement(set_enabled(8));
        assert!(core.plugin_parent_recovery_available("workspace", &a.id));
        let blocking_work = bounded_receiver.try_recv().unwrap();
        assert_eq!(blocking_work.key, b.id);
        assert_eq!(blocking_work.generation, blocker.generation);
        assert!(matches!(bounded_receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
        let recovery = core.submit_plugin_command("workspace", &a.id, set_enabled(8), &origin).unwrap();
        let recovery_work = bounded_receiver.try_recv().unwrap();
        assert_eq!(recovery_work.generation, prior_generation + 1);
        assert!(core.plugin_work_current(&recovery_work));
        assert!(matches!(bounded_receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
        {
            let mut owner = core.plugins_controller.lock().unwrap();
            owner.sender = Some(previous_sender);
            let publication = owner.publications.get_mut("workspace").unwrap();
            publication.operations.get_mut(&b.id).unwrap().result = Some(PluginsMutationResponse { plugin: Some(b.clone()), removed: false });
            let next = publication.clone();
            publication.settle_operation(&blocking_work.key, Ok(next), true, Some(true), &retained);
            owner.requests.get_mut(&("workspace".into(), b.id.clone())).unwrap().succeed(crate::core::ClientGeneration::new(blocking_work.generation), ());
        }
        assert!(matches!(recovery_work.intent, PluginIntent::SetEnabled { expected_revision: 8, enabled: false, .. }));
        assert_eq!(recovery.generation, recovery_work.generation);
        assert!(!core.plugin_parent_recovery_available("workspace", &a.id));
        {
            let mut owner = core.plugins_controller.lock().unwrap();
            let publication = owner.publications.get_mut("workspace").unwrap();
            publication.operations.get_mut(&a.id).unwrap().result = Some(PluginsMutationResponse {
                plugin: Some(PluginItem { revision: 8, ..a.clone() }), removed: false,
            });
            let next = publication.clone();
            publication.settle_operation(&recovery_work.key, Ok(next), true, Some(true), &retained);
        }
        // Retained upload-owner publications are inputs to this runtime boundary.
        // The upload owner's own regression covers confirmation/fingerprint
        // retention; here protect admission of that exact retry after ApplyUpdate.
        use crate::package_delivery::{operations::SkillUploadTarget, upload_flow::{SkillUploadPublication, SkillUploadState, PluginUpdatePreviewPublication}};
        let mut upload = SkillUploadPublication {
            operation_id: 42, generation: 1, revision: 0,
            mutation_uncertain: false, failure: None, plugin_preview: None, plugin_result: None,
            plugin_update_preview: Some(PluginUpdatePreviewPublication {
                upload_id: "exact-upload".into(),
                preview: PluginsUpdatePreviewResponse {
                    package: PluginsPreviewResponse { name: "A".into(), version: None, description: None, fingerprint: "exact-fingerprint".into(), components: vec![], diagnostics: vec![] },
                    supporting_files_changed: false,
                    added: vec![], updated: vec![], identity_resets: vec![], removed: vec![], authorization_changes: vec![],
                },
            }),
            state: SkillUploadState::Succeeded, sent_bytes: 1, total_bytes: 1,
            target: SkillUploadTarget::PluginUpdatePreview { plugin_id: a.id.clone(), expected_revision: 8 }, pack: false,
        };
        let publish_upload = |mut upload: SkillUploadPublication| {
            let scope = ClientScope::SkillsUpload { workspace_id: "workspace".into(), operation_id: upload.operation_id };
            upload.revision = core.snapshot(&scope).map_or(1, |snapshot| snapshot.revisions().scoped().get() + 1);
            core.publish(&ClientMutationAuthority { _private: () }, scope, crate::threads::registry::revisions(upload.revision), Arc::new(upload), vec![]);
        };
        publish_upload(upload.clone());
        core.submit_plugin_command("workspace", &a.id, PluginIntent::ApplyUpdate { upload_operation: 42, confirm_changes: true }, &origin).unwrap();
        let update_work = receiver.try_recv().unwrap();
        {
            let mut owner = core.plugins_controller.lock().unwrap();
            owner.publications.get_mut("workspace").unwrap().settle_operation(&update_work.key, Err(anyhow::anyhow!("update lost response")), true, None, &retained);
        }
        upload.mutation_uncertain = true;
        publish_upload(upload.clone());
        rejected_without_replacement(set_enabled(8));
        observe(Some(8));
        assert!(!core.plugin_parent_recovery_available("workspace", &a.id));
        rejected_without_replacement(set_enabled(8));
        rejected_without_replacement(PluginIntent::ConfigureMcp { server_id: "native-child".into(), body: "{}".into() });
        let mut new_upload = upload.clone();
        new_upload.operation_id = 43;
        new_upload.mutation_uncertain = false;
        publish_upload(new_upload);
        rejected_without_replacement(PluginIntent::ApplyUpdate { upload_operation: 43, confirm_changes: true });
        core.submit_plugin_command("workspace", &a.id, PluginIntent::ApplyUpdate { upload_operation: 42, confirm_changes: true }, &origin).unwrap();
        assert!(matches!(receiver.try_recv().unwrap().intent, PluginIntent::ApplyUpdate { upload_operation: 42, confirm_changes: true }));
        let exact = core.skill_upload_snapshot("workspace", 42).unwrap();
        assert_eq!(exact.plugin_update_preview.as_ref().unwrap().upload_id, "exact-upload");
        assert_eq!(exact.plugin_update_preview.as_ref().unwrap().preview.package.fingerprint, "exact-fingerprint");
        drop(second);

    }
}
