//! Shared capability operation execution and package delivery.
//! Skills and Plugins submit domain adapters to the same bounded owner; neither
//! domain owns the other domain's upload lifetime or archive bytes.
use super::{archive::*, upload_flow::*};
use crate::core::*;
use pioneer_protocol::{SkillId, SkillPackId};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Weak, mpsc},
};

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SkillsIntent {
    Policy {
        skill_id: SkillId,
        enabled: bool,
        allow_implicit_invocation: bool,
    },
    Remove {
        skill_id: SkillId,
    },
    RemovePack {
        pack_id: SkillPackId,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SkillUploadTarget {
    PluginInstall,
    PluginPreview,
    PluginUpdatePreview {
        plugin_id: String,
        expected_revision: i64,
    },
    Install,
    Update {
        skill_id: SkillId,
    },
    UpdatePack {
        pack_id: SkillPackId,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillsActionState {
    Pending,
    Succeeded,
    Failed,
    Cancelled,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SkillsActionPublication {
    pub operation_id: u64,
    pub revision: u64,
    pub state: SkillsActionState,
    pub kind: SkillsActionKind,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillsActionKind {
    Policy,
    Remove,
    RemovePack,
}
impl SkillsIntent {
    fn kind(&self) -> SkillsActionKind {
        match self {
            Self::Policy { .. } => SkillsActionKind::Policy,
            Self::Remove { .. } => SkillsActionKind::Remove,
            Self::RemovePack { .. } => SkillsActionKind::RemovePack,
        }
    }
}
struct Action {
    workspace: String,
    target: String,
    operation: u64,
    epoch: (u64, u64, Option<u64>),
    intent: SkillsIntent,
    previous_policy: Option<(bool, bool)>,
}
struct Upload {
    flow: SkillUploadFlow,
    epoch: (u64, u64, Option<u64>),
    target: SkillUploadTarget,
    source_kind: Option<SkillUploadSourceKind>,
    connection_bound: bool,
    confirming: bool,
}
enum Work {
    Action(Action),
    Prepare {
        workspace: String,
        operation: u64,
        path: PathBuf,
    },
    Abort {
        workspace: String,
        upload: String,
        epoch: (u64, u64, Option<u64>),
    },
}
#[derive(Default)]
pub(crate) struct CapabilityOperations {
    pub(crate) fenced: bool,
    next: u64,
    publications: BTreeMap<(String, String), Arc<SkillsActionPublication>>,
    uploads: BTreeMap<(String, u64), Upload>,
    queue: Option<pioneer_observability::queues::QueueObserver>,
    sender: Option<mpsc::SyncSender<pioneer_observability::work::WorkItem<Work>>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl CapabilityOperations {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.publications.clear();
        self.uploads.clear();
    }
}
impl Drop for CapabilityOperations {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
/// A consumer owns cancellation through this handle; the Client owns bytes and reduction.
pub struct SkillUploadOperation {
    core: Weak<ClientCore>,
    workspace: String,
    id: u64,
}
impl SkillUploadOperation {
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn cancel(&self) {
        if let Some(core) = self.core.upgrade() {
            core.cancel_skill_upload(&self.workspace, self.id);
        }
    }
}
impl Drop for SkillUploadOperation {
    fn drop(&mut self) {
        self.cancel();
    }
}
impl ClientCore {
    pub fn skills_action_snapshot(
        &self,
        workspace: &str,
        target: &str,
    ) -> Option<Arc<SkillsActionPublication>> {
        self.snapshot(&ClientScope::SkillsAction {
            workspace_id: workspace.into(),
            target: target.into(),
        })
        .and_then(|p| p.snapshot().payload())
    }
    pub fn skill_upload_snapshot(
        &self,
        workspace: &str,
        operation: u64,
    ) -> Option<Arc<SkillUploadPublication>> {
        self.snapshot(&ClientScope::SkillsUpload {
            workspace_id: workspace.into(),
            operation_id: operation,
        })
        .and_then(|p| p.snapshot().payload())
    }
    pub fn skills_intent(&self, workspace: &str, intent: SkillsIntent) -> anyhow::Result<u64> {
        self.skills_intent_admit(workspace, intent, None)
    }
    /// Plugin delegation shares native validation, queueing and completion. It
    /// must never recapture a Gateway selected after the originating read.
    pub(crate) fn skills_intent_bound(
        &self,
        workspace: &str,
        intent: SkillsIntent,
        epoch: (u64, u64, Option<u64>),
    ) -> anyhow::Result<u64> {
        self.skills_intent_admit(workspace, intent, Some(epoch))
    }
    fn skills_intent_admit(
        &self,
        workspace: &str,
        mut intent: SkillsIntent,
        expected_epoch: Option<(u64, u64, Option<u64>)>,
    ) -> anyhow::Result<u64> {
        anyhow::ensure!(
            self.capability_management_allowed(workspace),
            "capability_access_denied"
        );
        let epoch = expected_epoch.unwrap_or_else(|| self.provider_runtime_epoch());
        anyhow::ensure!(
            expected_epoch.is_none() || self.provider_runtime_epoch() == epoch,
            "connection_changed"
        );
        anyhow::ensure!(epoch.2.is_some(), "gateway_not_connected");
        let catalog = self
            .skills_catalog_snapshot(workspace)
            .ok_or_else(|| anyhow::anyhow!("skills_catalog_unavailable"))?;
        if let SkillsIntent::Policy {
            skill_id,
            allow_implicit_invocation,
            ..
        } = &mut intent
        {
            let skill = catalog
                .catalog
                .iter()
                .find(|s| &s.skill_id == skill_id)
                .ok_or_else(|| anyhow::anyhow!("skill_unavailable"))?;
            *allow_implicit_invocation =
                crate::skills::actions::effective_allow_implicit_invocation(
                    *allow_implicit_invocation,
                    Some(skill.policy.allow_implicit_invocation_editable),
                );
        }
        let target = match &intent {
            SkillsIntent::Policy { skill_id, .. } | SkillsIntent::Remove { skill_id } => {
                let skill = catalog
                    .catalog
                    .iter()
                    .find(|s| &s.skill_id == skill_id)
                    .ok_or_else(|| anyhow::anyhow!("skill_unavailable"))?;
                if matches!(intent, SkillsIntent::Remove { .. }) {
                    anyhow::ensure!(
                        skill.install.lifecycle_editable,
                        "skill_lifecycle_read_only"
                    );
                }
                skill_id.to_string()
            }
            SkillsIntent::RemovePack { pack_id } => {
                anyhow::ensure!(
                    catalog
                        .management
                        .packs
                        .iter()
                        .any(|p| &p.pack.id == pack_id),
                    "skill_pack_unavailable"
                );
                pack_id.to_string()
            }
        };
        let previous_policy = match &intent {
            SkillsIntent::Policy { skill_id, .. } => catalog
                .catalog
                .iter()
                .find(|s| &s.skill_id == skill_id)
                .map(|s| (s.policy.enabled, s.policy.allow_implicit_invocation)),
            _ => None,
        };
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        anyhow::ensure!(!owner.fenced, "capability_access_denied");
        anyhow::ensure!(
            !owner
                .publications
                .get(&(workspace.into(), target.clone()))
                .is_some_and(|p| p.state == SkillsActionState::Pending),
            "skill_action_pending"
        );
        anyhow::ensure!(
            expected_epoch.is_none() || self.provider_runtime_epoch() == epoch,
            "connection_changed"
        );
        owner.next = owner
            .next
            .checked_add(1)
            .expect("Skills operation identity exhausted");
        let operation = owner.next;
        anyhow::ensure!(owner.sender.is_some(), "client_stopped");
        send_skill_work(
            &owner,
            pioneer_observability::work::WorkItem::new(
                Work::Action(Action {
                    workspace: workspace.into(),
                    target: target.clone(),
                    operation,
                    epoch,
                    intent: intent.clone(),
                    previous_policy,
                }),
                pioneer_observability::work::Boundary::StdThread,
                pioneer_observability::work::WorkKind::ClientCommand,
                pioneer_observability::work::ExecutorRole::ClientController,
            ),
        )
        .map_err(|_| anyhow::anyhow!("skills_action_backpressure"))?;
        self.publish_skills_action(
            &mut owner,
            workspace,
            &target,
            operation,
            SkillsActionState::Pending,
            intent.kind(),
        );
        if let SkillsIntent::Policy {
            skill_id,
            enabled,
            allow_implicit_invocation,
        } = intent
        {
            self.apply_skills_policy(workspace, &skill_id, enabled, allow_implicit_invocation);
        }
        Ok(operation)
    }
    fn publish_skills_action(
        &self,
        owner: &mut CapabilityOperations,
        workspace: &str,
        target: &str,
        operation: u64,
        state: SkillsActionState,
        kind: SkillsActionKind,
    ) {
        let key = (workspace.into(), target.into());
        if owner
            .publications
            .get(&key)
            .is_some_and(|p| p.operation_id == operation && p.state == state)
        {
            return;
        }
        let scope = ClientScope::SkillsAction {
            workspace_id: workspace.into(),
            target: target.into(),
        };
        let revision = self
            .snapshot(&scope)
            .map_or(0, |p| p.revisions().scoped().get())
            .checked_add(1)
            .expect("Skill action revision exhausted");
        let p = Arc::new(SkillsActionPublication {
            operation_id: operation,
            revision,
            state,
            kind,
        });
        owner.publications.insert(key, p.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            p,
            vec![],
        );
    }
    fn skills_action_current(&self, action: &Action) -> bool {
        self.provider_runtime_epoch() == action.epoch
            && self.capability_management_allowed(&action.workspace)
            && {
                let owner = self
                    .capability_operations
                    .lock()
                    .expect("Skills controller poisoned");
                !owner.fenced
                    && owner
                        .publications
                        .get(&(action.workspace.clone(), action.target.clone()))
                        .is_some_and(|p| {
                            p.operation_id == action.operation
                                && p.state == SkillsActionState::Pending
                        })
            }
    }
    fn retire_skills_action(&self, action: &Action) {
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        if !owner
            .publications
            .get(&(action.workspace.clone(), action.target.clone()))
            .is_some_and(|p| {
                p.operation_id == action.operation && p.state == SkillsActionState::Pending
            })
        {
            return;
        }
        self.publish_skills_action(
            &mut owner,
            &action.workspace,
            &action.target,
            action.operation,
            SkillsActionState::Cancelled,
            action.intent.kind(),
        );
        drop(owner);
        if self.capability_management_allowed(&action.workspace) {
            self.fence_skills_reads_after_action(&action.workspace);
        }
    }
    fn complete_skills_action(&self, action: Action, result: anyhow::Result<()>) {
        if !self.skills_action_current(&action) {
            self.retire_skills_action(&action);
            return;
        }
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        if !owner
            .publications
            .get(&(action.workspace.clone(), action.target.clone()))
            .is_some_and(|p| {
                p.operation_id == action.operation && p.state == SkillsActionState::Pending
            })
        {
            return;
        }
        // Fence the Pending read generation before exposing settlement or
        // rollback. Read policy uses committed projection without taking I.
        self.fence_skills_reads_after_action(&action.workspace);
        self.publish_skills_action(
            &mut owner,
            &action.workspace,
            &action.target,
            action.operation,
            if result.is_ok() {
                SkillsActionState::Succeeded
            } else {
                SkillsActionState::Failed
            },
            action.intent.kind(),
        );
        if result.is_err()
            && let SkillsIntent::Policy { skill_id, .. } = &action.intent
            && let Some((enabled, implicit)) = action.previous_policy
        {
            self.apply_skills_policy(&action.workspace, skill_id, enabled, implicit);
        }
        drop(owner);
    }
    /// The path is the explicit result of a shell picker. No path or bytes enter publications.
    pub fn start_skill_upload(
        self: &Arc<Self>,
        workspace: &str,
        target: SkillUploadTarget,
        path: PathBuf,
    ) -> anyhow::Result<SkillUploadOperation> {
        self.start_skill_upload_bound(workspace, target, path, None)
    }
    /// File shells bind asynchronous picker results to their original Gateway;
    /// desktop synchronous pickers may retain the legacy None admission.
    pub fn start_skill_upload_bound(
        self: &Arc<Self>,
        workspace: &str,
        target: SkillUploadTarget,
        path: PathBuf,
        expected_connection: Option<u64>,
    ) -> anyhow::Result<SkillUploadOperation> {
        anyhow::ensure!(
            self.capability_management_allowed(workspace),
            "capability_access_denied"
        );
        let epoch = self.provider_runtime_epoch();
        anyhow::ensure!(epoch.2.is_some(), "gateway_not_connected");
        anyhow::ensure!(
            expected_connection.is_none_or(|c| epoch.2 == Some(c)),
            "gateway_changed_during_file_selection"
        );
        if !matches!(
            target,
            SkillUploadTarget::Install
                | SkillUploadTarget::PluginInstall
                | SkillUploadTarget::PluginPreview
                | SkillUploadTarget::PluginUpdatePreview { .. }
        ) {
            let catalog = self
                .skills_catalog_snapshot(workspace)
                .ok_or_else(|| anyhow::anyhow!("skills_catalog_unavailable"))?;
            match &target {
                SkillUploadTarget::Update { skill_id } => anyhow::ensure!(
                    catalog
                        .installed
                        .iter()
                        .any(|s| &s.skill_id == skill_id && s.install.lifecycle_editable),
                    "skill_lifecycle_read_only"
                ),
                SkillUploadTarget::UpdatePack { pack_id } => anyhow::ensure!(
                    catalog
                        .management
                        .packs
                        .iter()
                        .any(|p| &p.pack.id == pack_id),
                    "skill_pack_unavailable"
                ),
                SkillUploadTarget::Install
                | SkillUploadTarget::PluginInstall
                | SkillUploadTarget::PluginPreview
                | SkillUploadTarget::PluginUpdatePreview { .. } => {}
            }
        }
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        anyhow::ensure!(!owner.fenced, "capability_access_denied");
        anyhow::ensure!(
            !owner
                .uploads
                .iter()
                .any(|((w, _), u)| w == workspace && !u.flow.publication.state.is_terminal()),
            "skill_upload_pending"
        );
        owner.uploads.retain(|_, upload| {
            !upload.flow.publication.state.is_terminal()
                || upload.flow.publication.plugin_preview.is_some()
                || upload.flow.publication.plugin_update_preview.is_some()
        });
        owner.next = owner
            .next
            .checked_add(1)
            .expect("Skills operation identity exhausted");
        let operation = owner.next;
        anyhow::ensure!(owner.sender.is_some(), "client_stopped");
        send_skill_work(
            &owner,
            pioneer_observability::work::WorkItem::new(
                Work::Prepare {
                    workspace: workspace.into(),
                    operation,
                    path,
                },
                pioneer_observability::work::Boundary::StdThread,
                pioneer_observability::work::WorkKind::ClientCommand,
                pioneer_observability::work::ExecutorRole::ClientController,
            ),
        )
        .map_err(|_| anyhow::anyhow!("skills_upload_backpressure"))?;
        let connection_bound = expected_connection.is_some()
            || matches!(
                target,
                SkillUploadTarget::PluginInstall
                    | SkillUploadTarget::PluginPreview
                    | SkillUploadTarget::PluginUpdatePreview { .. }
            );
        let mut upload = Upload {
            confirming: false,
            connection_bound,
            flow: SkillUploadFlow::new(operation, workspace.into()),
            epoch,
            target,
            source_kind: None,
        };
        upload.flow.publication.target = upload.target.clone();
        self.publish_skill_upload(workspace, &mut upload);
        owner.uploads.insert((workspace.into(), operation), upload);
        Ok(SkillUploadOperation {
            core: Arc::downgrade(self),
            workspace: workspace.into(),
            id: operation,
        })
    }
    fn skill_upload_connection(&self, workspace: &str, operation: u64) -> Option<u64> {
        self.capability_operations
            .lock()
            .expect("Skills controller poisoned")
            .uploads
            .get(&(workspace.into(), operation))
            .and_then(|u| u.epoch.2)
    }
    fn admit_plugin_install(
        &self,
        workspace: &str,
        operation: u64,
        effect_generation: u64,
        connection: Option<u64>,
        upload_id: String,
        package: pioneer_protocol::PluginsPreviewResponse,
    ) -> anyhow::Result<()> {
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        let upload = owner
            .uploads
            .get_mut(&(workspace.into(), operation))
            .ok_or_else(|| anyhow::anyhow!("plugin_preview_stale"))?;
        anyhow::ensure!(
            self.upload_current(workspace, upload)
                && upload.epoch.2 == connection
                && upload.target == SkillUploadTarget::PluginInstall
                && upload.flow.publication.state == SkillUploadState::Applying
                && upload.flow.accepts_effect(effect_generation),
            "plugin_preview_stale"
        );
        // Preview is read-only. Admit the new mutation under its original
        // owner only after the response; retirement that won this lock blocks
        // install. Once admitted, retain exact retry identity if its result is lost.
        upload.flow.publication.plugin_preview =
            Some(super::upload_flow::PluginPreviewPublication { upload_id, package });
        Ok(())
    }
    pub(crate) fn confirm_plugin_upload(
        &self,
        workspace: &str,
        operation: u64,
        epoch: (u64, u64, Option<u64>),
    ) -> anyhow::Result<SkillUploadPublication> {
        anyhow::ensure!(
            self.provider_runtime_epoch() == epoch,
            "plugin_preview_stale"
        );
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        let u = owner
            .uploads
            .get_mut(&(workspace.into(), operation))
            .ok_or_else(|| anyhow::anyhow!("plugin_preview_required"))?;
        anyhow::ensure!(
            u.epoch == epoch
                && !u.confirming
                && (u.flow.publication.state == SkillUploadState::Succeeded
                    || u.flow.publication.mutation_uncertain),
            "plugin_preview_stale"
        );
        anyhow::ensure!(
            u.flow.publication.plugin_preview.is_some()
                || u.flow.publication.plugin_update_preview.is_some(),
            "plugin_preview_required"
        );
        u.confirming = true;
        // Retain the exact upload/fingerprint across a lost response. Gateway
        // deduplicates install by source_upload_id; a new upload is not a retry.
        Ok(u.flow.publication.clone())
    }
    pub(crate) fn complete_plugin_upload_confirmation(
        &self,
        workspace: &str,
        operation: u64,
        epoch: (u64, u64, Option<u64>),
        succeeded: bool,
    ) {
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        if let Some(upload) = owner.uploads.get_mut(&(workspace.into(), operation))
            && upload.epoch == epoch
        {
            upload.confirming = false;
            if succeeded {
                upload.flow.publication.mutation_uncertain = false;
                upload.flow.publication.state = SkillUploadState::Succeeded;
                upload.flow.publication.failure = None;
                upload.flow.publication.plugin_preview = None;
                upload.flow.publication.plugin_update_preview = None;
            } else {
                upload.flow.publication.mutation_uncertain = true;
            }
            self.publish_skill_upload(workspace, upload);
        }
    }
    fn publish_skill_upload(&self, workspace: &str, upload: &mut Upload) {
        let scope = ClientScope::SkillsUpload {
            workspace_id: workspace.into(),
            operation_id: upload.flow.publication.operation_id,
        };
        if self
            .skill_upload_snapshot(workspace, upload.flow.publication.operation_id)
            .is_some_and(|p| *p == upload.flow.publication)
        {
            return;
        }
        upload.flow.publication.revision = self
            .snapshot(&scope)
            .map_or(0, |p| p.revisions().scoped().get())
            .checked_add(1)
            .expect("Upload revision exhausted");
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(upload.flow.publication.revision),
            Arc::new(upload.flow.publication.clone()),
            vec![],
        );
    }
    fn upload_current(&self, workspace: &str, upload: &Upload) -> bool {
        self.provider_runtime_epoch() == upload.epoch
            && self.capability_management_allowed(workspace)
            && !upload.flow.publication.state.is_terminal()
    }
    pub fn cancel_skill_upload(&self, workspace: &str, operation: u64) {
        let mut owner = self
            .capability_operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(upload) = owner.uploads.get_mut(&(workspace.into(), operation)) else {
            return;
        };
        // Once Apply is dispatched, cancellation cannot establish whether the
        // remote mutation committed. Keep observing its authoritative result.
        if upload.confirming
            || upload.flow.publication.mutation_uncertain
            || upload.flow.publication.state == SkillUploadState::Applying
        {
            return;
        }
        let epoch = upload.epoch;
        upload.flow.terminate(SkillUploadState::Cancelled);
        let remote = upload
            .flow
            .take_cleanup()
            .or_else(|| {
                upload
                    .flow
                    .publication
                    .plugin_preview
                    .take()
                    .map(|p| p.upload_id)
            })
            .or_else(|| {
                upload
                    .flow
                    .publication
                    .plugin_update_preview
                    .take()
                    .map(|p| p.upload_id)
            });
        self.publish_skill_upload(workspace, upload);
        if let Some(remote) = remote
            && owner.sender.is_some()
        {
            let _ = send_skill_work(
                &owner,
                pioneer_observability::work::WorkItem::new(
                    Work::Abort {
                        workspace: workspace.into(),
                        upload: remote,
                        epoch,
                    },
                    pioneer_observability::work::Boundary::StdThread,
                    pioneer_observability::work::WorkKind::ClientCommand,
                    pioneer_observability::work::ExecutorRole::ClientController,
                ),
            );
        }
    }
    pub(crate) fn invalidate_skills_operations(&self) {
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        owner.fenced = true;
        let pending = owner
            .publications
            .iter()
            .filter(|(_, p)| p.state == SkillsActionState::Pending)
            .map(|((w, t), p)| (w.clone(), t.clone(), p.operation_id, p.kind))
            .collect::<Vec<_>>();
        for (w, t, id, kind) in pending {
            self.publish_skills_action(&mut owner, &w, &t, id, SkillsActionState::Cancelled, kind);
        }
        for ((workspace, _), upload) in &mut owner.uploads {
            upload.flow.terminate(SkillUploadState::Cancelled);
            self.publish_skill_upload(workspace, upload);
        }
        // The enclosing identity transaction evicts protected publications.
        // Old handles/workers must not republish prior-principal previews later.
        // No abort is sent: an already dispatched mutation may still commit.
        owner.uploads.clear();
    }
    fn retire_skill_upload(&self, workspace: &str, upload: &mut Upload) -> bool {
        if upload.flow.publication.state.is_terminal() {
            return false;
        }
        upload.flow.terminate(SkillUploadState::Cancelled);
        self.publish_skill_upload(workspace, upload);
        true
    }
    fn skill_preparation_target(
        &self,
        workspace: &str,
        operation: u64,
    ) -> Option<SkillUploadTarget> {
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        let upload = owner.uploads.get_mut(&(workspace.into(), operation))?;
        if !self.upload_current(workspace, upload) {
            let retired = self.retire_skill_upload(workspace, upload);
            drop(owner);
            if retired && self.capability_management_allowed(workspace) {
                self.fence_skills_reads_after_action(workspace);
            }
            return None;
        }
        Some(upload.target.clone())
    }
    fn complete_skill_preparation(
        &self,
        workspace: &str,
        operation: u64,
        prepared: anyhow::Result<(SkillUploadSourceKind, SkillUploadArchive)>,
    ) {
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        let Some(upload) = owner.uploads.get_mut(&(workspace.into(), operation)) else {
            return;
        };
        if upload.flow.publication.state != SkillUploadState::Preparing {
            return;
        }
        if !self.upload_current(workspace, upload) {
            let retired = self.retire_skill_upload(workspace, upload);
            drop(owner);
            if retired && self.capability_management_allowed(workspace) {
                self.fence_skills_reads_after_action(workspace);
            }
            return;
        }
        match prepared {
            Ok((kind, archive)) => {
                upload.source_kind = Some(kind);
                upload.flow.publication.pack = kind == SkillUploadSourceKind::Pack;
                upload.flow.prepare(archive);
            }
            Err(error) => {
                upload.flow.fail(SkillUploadFailure::from_error(&error));
            }
        }
        self.publish_skill_upload(workspace, upload);
    }
    fn next_skill_upload_effect(
        &self,
        workspace: &str,
        operation: u64,
    ) -> Option<(
        UploadEffect,
        SkillUploadTarget,
        SkillUploadSourceKind,
        u64,
        bool,
    )> {
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        let upload = owner.uploads.get_mut(&(workspace.into(), operation))?;
        if !self.upload_current(workspace, upload) {
            let retired = self.retire_skill_upload(workspace, upload);
            drop(owner);
            if retired && self.capability_management_allowed(workspace) {
                self.fence_skills_reads_after_action(workspace);
            }
            return None;
        }
        Some((
            upload.flow.effect()?,
            upload.target.clone(),
            upload.source_kind?,
            upload.flow.effect_generation(),
            upload.connection_bound,
        ))
    }
    fn complete_skill_upload_effect(
        &self,
        workspace: &str,
        operation: u64,
        effect_generation: u64,
        result: anyhow::Result<UploadCompletion>,
    ) {
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        let Some(upload) = owner.uploads.get_mut(&(workspace.into(), operation)) else {
            return;
        };
        if !upload.flow.accepts_effect(effect_generation) {
            return;
        }
        if !self.upload_current(workspace, upload) {
            let retired = self.retire_skill_upload(workspace, upload);
            drop(owner);
            if retired && self.capability_management_allowed(workspace) {
                self.fence_skills_reads_after_action(workspace);
            }
            return;
        }
        // Only this explicit Plugin mutation owns a direct parent refresh.
        // Standalone/preview completions use authorized domain notifications.
        let plugin_install_dispatched = upload.target == SkillUploadTarget::PluginInstall
            && upload.flow.publication.state == SkillUploadState::Applying;
        let failed = result.is_err();
        match result {
            Ok(completion) => {
                if !upload.flow.complete(effect_generation, completion) {
                    return;
                }
            }
            Err(error) => {
                if upload.target == SkillUploadTarget::PluginInstall
                    && upload.flow.publication.state == SkillUploadState::Applying
                    && upload.flow.publication.plugin_preview.is_some()
                {
                    upload
                        .flow
                        .fail_uncertain(SkillUploadFailure::from_error(&error));
                } else {
                    upload.flow.fail(SkillUploadFailure::from_error(&error));
                }
            }
        }
        let plugin_install_uncertain =
            plugin_install_dispatched && upload.flow.publication.mutation_uncertain;
        self.publish_skill_upload(workspace, upload);
        let succeeded = upload.flow.publication.state == SkillUploadState::Succeeded;
        let cleanup = upload.flow.take_cleanup();
        let epoch = upload.epoch;
        if let Some(upload) = cleanup
            && owner.sender.is_some()
        {
            let _ = send_skill_work(
                &owner,
                pioneer_observability::work::WorkItem::new(
                    Work::Abort {
                        workspace: workspace.into(),
                        upload,
                        epoch,
                    },
                    pioneer_observability::work::Boundary::StdThread,
                    pioneer_observability::work::WorkKind::ClientCommand,
                    pioneer_observability::work::ExecutorRole::ClientController,
                ),
            );
        }
        drop(owner);
        if succeeded || failed {
            self.fence_skills_reads_after_action(workspace);
            if plugin_install_uncertain {
                self.refresh_plugin_publication(workspace);
            }
        }
    }
    pub(crate) fn start_skills_operation_controller(self: &Arc<Self>) {
        let (sender, receiver) =
            mpsc::sync_channel::<pioneer_observability::work::WorkItem<Work>>(32);
        let queue = pioneer_observability::queues::QueueObserver::new(
            pioneer_observability::queues::QueueKind::ClientController,
            Some(32),
            "skills work items; payload bytes unavailable without traversal",
        );
        let receiver_queue = queue.clone();
        let weak = Arc::downgrade(self);
        let task=std::thread::Builder::new().name("client-skills-actions".into()).spawn(move||while let Ok(item)=receiver.recv(){
            let (work,carrier)=item.received(pioneer_observability::work::Boundary::StdThread,pioneer_observability::work::WorkKind::ClientCommand,pioneer_observability::work::ExecutorRole::ClientController);let _scope=carrier.enter();
            let mut active = pioneer_observability::live::WorkGuard::start(carrier.context(), false);
            let mut processing = receiver_queue.processing();
            let mut outcome=pioneer_observability::work::Outcome::Completed;
            match work {
                Work::Action(action)=>{
                    let Some(core)=weak.upgrade() else{return;};if !core.skills_action_current(&action){core.retire_skills_action(&action);continue;}let sender=core.transport_runtime().ws_command_sender();drop(core);
                    let bound=sender.requests_for_connection(action.epoch.2.expect("admitted Skills action has a connection"));
                    let result=match &action.intent {
                        SkillsIntent::Policy{skill_id,enabled,allow_implicit_invocation}=>crate::transport::ws::command_sender::skills_policy_set(&bound,crate::skills::actions::skills_policy_set_params(&action.workspace,skill_id.clone(),*enabled,*allow_implicit_invocation)).map(|_|()),
                        SkillsIntent::Remove{skill_id}=>crate::transport::ws::command_sender::skills_uninstall(&bound,crate::skills::actions::skills_uninstall_params(&action.workspace,skill_id.clone())).map(|_|()),
                        SkillsIntent::RemovePack{pack_id}=>crate::transport::ws::command_sender::skills_pack_uninstall(&bound,crate::skills::actions::skills_pack_uninstall_params(&action.workspace,pack_id.clone())).map(|_|()),
                    };if result.is_err(){outcome=pioneer_observability::work::Outcome::Failed;} if let Some(core)=weak.upgrade(){core.complete_skills_action(action,result);}
                }
                Work::Abort{workspace,upload,epoch}=>{
                    let Some(core)=weak.upgrade() else{return;};if core.is_stopped()||core.provider_runtime_epoch()!=epoch||!core.capability_management_allowed(&workspace){continue;}let sender=core.transport_runtime().ws_command_sender();drop(core);if let Some(connection)=epoch.2 {if crate::transport::ws::command_sender::skills_upload_abort(&sender.requests_for_connection(connection),super::upload::skills_upload_abort_params(workspace,upload)).is_err(){outcome=pioneer_observability::work::Outcome::Failed;}}
                }
                Work::Prepare{workspace,operation,path}=>{
                    let Some(core)=weak.upgrade() else{return;};let target=core.skill_preparation_target(&workspace,operation);drop(core);let Some(target)=target else{continue;};
                    let prepared=prepare_archive_effect(target,path);
                    if prepared.is_err(){outcome=pioneer_observability::work::Outcome::Failed;}
                    if let Some(core)=weak.upgrade(){core.complete_skill_preparation(&workspace,operation,prepared);}else{return;}

                    loop {
                        let Some(core)=weak.upgrade() else{return;};let effect=core.next_skill_upload_effect(&workspace,operation);let connection=core.skill_upload_connection(&workspace,operation);let sender=core.transport_runtime().ws_command_sender();drop(core);let Some((effect,target,kind,effect_generation,connection_bound))=effect else{break;};
                        let bound = connection.map(|id| sender.requests_for_connection(id));
                        let result=match effect {
                            UploadEffect::Start(mut params)=>{
                                if matches!(target,SkillUploadTarget::PluginInstall|SkillUploadTarget::PluginPreview|SkillUploadTarget::PluginUpdatePreview{..}) {params.purpose=pioneer_protocol::SkillUploadPurpose::Plugin;}
                                if connection_bound {match &bound {Some(bound)=>crate::transport::ws::command_sender::skills_upload_start(bound,params),None=>Err(anyhow::anyhow!("gateway_not_connected"))}} else {sender.skills_upload_start(params)}
                            }.map(UploadCompletion::Start),
                            UploadEffect::Chunk{upload_id,offset,bytes}=>sender.send_skill_upload_chunk_bound(connection,workspace.clone(),upload_id,offset,bytes).map(UploadCompletion::Chunk),
                            UploadEffect::Finish(params)=>{ if connection_bound { match &bound { Some(bound) => crate::transport::ws::command_sender::skills_upload_finish(bound, params), None => Err(anyhow::anyhow!("gateway_not_connected")) } } else { sender.skills_upload_finish(params) } }.map(UploadCompletion::Finish),
                            UploadEffect::Apply{upload_id} if matches!(&target, SkillUploadTarget::PluginUpdatePreview {..}) => match &target {
                                SkillUploadTarget::PluginUpdatePreview {plugin_id,expected_revision} => match &bound {
                                    Some(bound)=>crate::transport::ws::command_sender::plugins_update_preview(bound,pioneer_protocol::PluginsUpdatePreviewParams {
                                        workspace_id:workspace.clone(),plugin_id:plugin_id.clone(),expected_revision:*expected_revision,upload_id:upload_id.clone()
                                    }).map(|preview|UploadCompletion::PluginUpdatePreviewed(upload_id,preview)),
                                    None=>Err(anyhow::anyhow!("gateway_not_connected")),
                                },_=>unreachable!(),
                            },
                            UploadEffect::Apply{upload_id} if target == SkillUploadTarget::PluginPreview => match &bound { Some(bound)=>crate::transport::ws::command_sender::plugins_preview(bound,pioneer_protocol::PluginsSourceParams{workspace_id:workspace.clone(),upload_id:upload_id.clone(),target:None}).map(|preview|UploadCompletion::PluginPreviewed(upload_id,preview)),None=>Err(anyhow::anyhow!("gateway_not_connected")) },
                            UploadEffect::Apply { upload_id } if target == SkillUploadTarget::PluginInstall => match &bound {
                                Some(bound) => crate::transport::ws::command_sender::plugins_preview(
                                    bound,
                                    pioneer_protocol::PluginsSourceParams { workspace_id: workspace.clone(), upload_id: upload_id.clone(), target: None },
                                ).and_then(|preview| {
                                    let core = weak.upgrade().ok_or_else(|| anyhow::anyhow!("client_stopped"))?;
                                    core.admit_plugin_install(&workspace, operation, effect_generation, connection, upload_id.clone(), preview.clone())?;
                                    crate::transport::ws::command_sender::plugins_install(bound, pioneer_protocol::PluginsInstallParams { workspace_id: workspace.clone(), upload_id, expected_fingerprint: preview.fingerprint })
                                }),
                                None => Err(anyhow::anyhow!("gateway_not_connected")),
                            }.map(UploadCompletion::PluginApplied),
                            UploadEffect::Apply{upload_id}=>match target {
                                SkillUploadTarget::PluginInstall|SkillUploadTarget::PluginPreview|SkillUploadTarget::PluginUpdatePreview {..}=>unreachable!("plugin upload handled above"),
                                SkillUploadTarget::Install=>match kind {SkillUploadSourceKind::Skill=>sender.skills_install(crate::skills::actions::skills_install_uploaded_archive_params(&workspace,upload_id)).map(|_|()),SkillUploadSourceKind::Plugin=>unreachable!("plugin source uses plugin target"),SkillUploadSourceKind::Pack=>sender.skills_pack_install(crate::skills::actions::skills_pack_install_uploaded_archive_params(&workspace,upload_id)).map(|_|())},
                                SkillUploadTarget::Update{skill_id}=>{
                                    let params=crate::skills::actions::skills_update_uploaded_archive_params(&workspace,skill_id,upload_id,None);
                                    if connection_bound {match &bound {Some(bound)=>crate::transport::ws::command_sender::skills_update(bound,params),None=>Err(anyhow::anyhow!("gateway_not_connected"))}}else{sender.skills_update(params)}.map(|_|())
                                },
                                SkillUploadTarget::UpdatePack{pack_id}=>sender.skills_pack_update(crate::skills::actions::skills_pack_update_uploaded_archive_params(&workspace,pack_id,upload_id)).map(|_|()),
                            }.map(|_|UploadCompletion::Applied),
                        };if result.is_err(){outcome=pioneer_observability::work::Outcome::Failed;} if let Some(core)=weak.upgrade(){core.complete_skill_upload_effect(&workspace,operation,effect_generation,result);}
                    }
                }
            }
            processing.processed();
            active.finish(outcome);
        }).expect("Skills operation controller could not start");
        let mut owner = self
            .capability_operations
            .lock()
            .expect("Skills controller poisoned");
        owner.queue = Some(queue);
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

fn send_skill_work(
    owner: &CapabilityOperations,
    mut item: pioneer_observability::work::WorkItem<Work>,
) -> Result<(), ()> {
    let Some(sender) = &owner.sender else {
        return Err(());
    };
    if let Some(queue) = &owner.queue {
        item = item.observed(
            queue.clone(),
            pioneer_observability::queues::QueueBytes::Unsupported,
        );
    }
    match sender.try_send(item) {
        Ok(()) => Ok(()),
        Err(error) => {
            let item = match error {
                mpsc::TrySendError::Full(item) | mpsc::TrySendError::Disconnected(item) => item,
            };
            if let Some((queue, bytes)) = &item.queue {
                queue.rejected_send(*bytes);
            }
            Err(())
        }
    }
}

/// Filesystem execution for the explicit directory selected by a shell. Tests inject archives.
fn prepare_archive_effect(
    target: SkillUploadTarget,
    path: PathBuf,
) -> anyhow::Result<(SkillUploadSourceKind, SkillUploadArchive)> {
    let kind = match target {
        SkillUploadTarget::PluginInstall
        | SkillUploadTarget::PluginPreview
        | SkillUploadTarget::PluginUpdatePreview { .. } => SkillUploadSourceKind::Plugin,
        SkillUploadTarget::Install => classify_skill_upload_source(&path)?,
        SkillUploadTarget::Update { .. } => SkillUploadSourceKind::Skill,
        SkillUploadTarget::UpdatePack { .. } => SkillUploadSourceKind::Pack,
    };
    // A phone can supply a cache file, not a directory. Preserve archive bytes
    // and defer extraction/security/modes to the same Gateway native installer.
    let archive = if kind != SkillUploadSourceKind::Plugin && path.is_file() {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        anyhow::ensure!(
            name.ends_with(".tar.gz") || name.ends_with(".tgz"),
            "skill_archive_required"
        );
        build_plugin_upload_archive(&path)?
    } else {
        match kind {
            SkillUploadSourceKind::Plugin => build_plugin_upload_archive(&path)?,
            SkillUploadSourceKind::Skill => build_skill_upload_archive(&path)?,
            SkillUploadSourceKind::Pack => build_skill_pack_upload_archive(&path)?,
        }
    };
    Ok((kind, archive))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_test_support::{client, skill};
    fn fixture() -> (
        Arc<ClientCore>,
        mpsc::Receiver<pioneer_observability::work::WorkItem<Work>>,
    ) {
        let core = client();
        core.accept_skills_catalog_for_test(
            "workspace",
            crate::skills::catalog::project_skills_snapshot(vec![skill('A'), skill('B')], vec![]),
        );
        let (sender, receiver) = mpsc::sync_channel(4);
        core.capability_operations.lock().unwrap().sender = Some(sender);
        (core, receiver)
    }
    #[test]
    fn authorization_publication_and_native_settlement_both_make_progress() {
        for failure in [false, true].into_iter().cycle().take(32) {
            let (core, receiver) = fixture();
            core.skills_intent(
                "workspace",
                SkillsIntent::Policy {
                    skill_id: skill('A').skill_id,
                    enabled: false,
                    allow_implicit_invocation: false,
                },
            )
            .unwrap();
            let Work::Action(work) = receiver.try_recv().unwrap().value else {
                panic!("expected native action")
            };
            assert!(core.skills_action_current(&work));
            let target = work.target.clone();
            assert_eq!(
                core.skills_action_snapshot("workspace", &target)
                    .unwrap()
                    .state,
                SkillsActionState::Pending
            );
            let publication = core
                .snapshot(&ClientScope::Administration { workspace_id: None })
                .unwrap();
            let mut revoked = (*publication.snapshot().payload::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>().unwrap()).clone();
            revoked.authorization_change_sequence += 1;
            revoked.capabilities.clear_epoch();
            let start = Arc::new(std::sync::Barrier::new(3));
            let (finished, observed) = mpsc::channel();
            let worker_core = core.clone();
            let worker_start = start.clone();
            let worker_finished = finished.clone();
            let worker = std::thread::spawn(move || {
                worker_start.wait();
                worker_core.complete_skills_action(
                    work,
                    if failure {
                        Err(anyhow::anyhow!("native failure"))
                    } else {
                        Ok(())
                    },
                );
                worker_finished.send(()).unwrap();
            });
            let publisher_core = core.clone();
            let publisher_start = start.clone();
            let publisher = std::thread::spawn(move || {
                let _identity = publisher_core.identity_authorization.lock().unwrap();
                publisher_start.wait();
                publisher_core.publish_identity_authorization(
                    &revoked,
                    crate::gateway::identity_authorization::IdentityPublicationChange::Revalidate,
                );
                finished.send(()).unwrap();
            });
            start.wait();
            // Bound a deadlock failure, but require actual publication AND
            // settlement acknowledgements plus joins, not a timeout as success.
            for _ in 0..2 {
                observed
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("publisher/settlement cycle");
            }
            publisher.join().unwrap();
            worker.join().unwrap();
            assert!(!core.capability_management_allowed("workspace"));
            assert!(core.skills_action_snapshot("workspace", &target).is_none());
        }
    }
    // NOT_RUN / NOT_COMPILED. Pause at delegation with copied native IDs.
    #[test]
    fn plugin_skill_admission_rejects_replacement_and_queued_work_keeps_origin() {
        for intent in [
            SkillsIntent::Policy {
                skill_id: skill('A').skill_id,
                enabled: false,
                allow_implicit_invocation: false,
            },
            SkillsIntent::Remove {
                skill_id: skill('A').skill_id,
            },
        ] {
            let (core, receiver) = fixture();
            let origin = core.provider_runtime_epoch();
            let pause = Arc::new(std::sync::Barrier::new(2));
            let resume = Arc::new(std::sync::Barrier::new(2));
            let worker_core = core.clone();
            let worker_pause = pause.clone();
            let worker_resume = resume.clone();
            let worker_intent = intent.clone();
            let worker = std::thread::spawn(move || {
                worker_pause.wait();
                worker_resume.wait();
                worker_core.skills_intent_bound("workspace", worker_intent, origin)
            });
            pause.wait();
            crate::catalog_test_support::switch_connection(&core, 8);
            resume.wait();
            assert_eq!(
                worker.join().unwrap().unwrap_err().to_string(),
                "connection_changed"
            );
            assert!(receiver.try_recv().is_err());
            assert!(
                core.skills_catalog_snapshot("workspace").unwrap().catalog[0]
                    .policy
                    .enabled
            );
            // Same native operation, accepted on its own current origin; then a
            // second replacement before send cannot retarget the queued action.
            let admitted = core.provider_runtime_epoch();
            core.skills_intent_bound("workspace", intent, admitted)
                .unwrap();
            let Work::Action(action) = receiver.try_recv().unwrap().value else {
                panic!()
            };
            crate::catalog_test_support::switch_connection(&core, 9);
            assert_eq!(action.epoch, admitted);
            assert!(!core.skills_action_current(&action));
        }
    }
    #[test]
    fn retired_ticket_releases_action_and_archive_without_touching_replacements() {
        let (core, queue) = fixture();
        let skill_id = skill('A').skill_id;
        let intent = || SkillsIntent::Remove {
            skill_id: skill_id.clone(),
        };
        core.skills_intent("workspace", intent()).unwrap();
        let Work::Action(action) = queue.try_recv().unwrap().value else {
            panic!()
        };
        let late = Action {
            workspace: action.workspace.clone(),
            target: action.target.clone(),
            operation: action.operation,
            epoch: action.epoch,
            intent: intent(),
            previous_policy: None,
        };
        let upload = core
            .start_skill_upload("workspace", SkillUploadTarget::Install, "unused".into())
            .unwrap();
        let _ = queue.try_recv().unwrap();
        crate::catalog_test_support::replace_connection(&core, 8);
        core.complete_skills_action(action, Ok(()));
        assert_eq!(
            core.skills_action_snapshot("workspace", &skill_id.to_string())
                .unwrap()
                .state,
            SkillsActionState::Cancelled
        );
        assert!(
            core.skill_preparation_target("workspace", upload.id())
                .is_none()
        );
        assert_eq!(
            core.skill_upload_snapshot("workspace", upload.id())
                .unwrap()
                .state,
            SkillUploadState::Cancelled
        );
        core.skills_intent("workspace", intent()).unwrap();
        let pending = core
            .skills_action_snapshot("workspace", &skill_id.to_string())
            .unwrap();
        let next = core
            .start_skill_upload("workspace", SkillUploadTarget::Install, "unused".into())
            .unwrap();
        core.complete_skills_action(late, Err(anyhow::anyhow!("late")));
        core.complete_skill_preparation("workspace", upload.id(), Err(anyhow::anyhow!("late")));
        assert!(Arc::ptr_eq(
            &pending,
            &core
                .skills_action_snapshot("workspace", &skill_id.to_string())
                .unwrap()
        ));
        assert_eq!(
            core.skill_upload_snapshot("workspace", next.id())
                .unwrap()
                .state,
            SkillUploadState::Preparing
        );
    }
    #[test]
    fn skill_policy_remove_failure_retry_and_access_fence_share_one_controller() {
        let (core, receiver) = fixture();
        let (_plugins, assert_plugins_quiet) =
            crate::plugins::runtime::tests::watch_plugin_reads(&core);
        let id = skill('A').skill_id;
        let intent = || SkillsIntent::Policy {
            skill_id: id.clone(),
            enabled: false,
            allow_implicit_invocation: false,
        };
        let first = core.skills_intent("workspace", intent()).unwrap();
        assert!(core.skills_intent("workspace", intent()).is_err());
        let Work::Action(action) = receiver.try_recv().unwrap().value else {
            panic!()
        };
        core.complete_skills_action(action, Err(anyhow::anyhow!("synthetic failure")));
        assert_eq!(
            core.skills_action_snapshot("workspace", &id.to_string())
                .unwrap()
                .state,
            SkillsActionState::Failed
        );
        assert!(core.skills_intent("workspace", intent()).unwrap() > first);
        let Work::Action(action) = receiver.try_recv().unwrap().value else {
            panic!()
        };
        core.complete_skills_action(action, Ok(()));
        core.skills_intent(
            "workspace",
            SkillsIntent::Remove {
                skill_id: id.clone(),
            },
        )
        .unwrap();
        let Work::Action(action) = receiver.try_recv().unwrap().value else {
            panic!()
        };
        core.invalidate_skills_operations();
        let before = core
            .skills_action_snapshot("workspace", &id.to_string())
            .unwrap();
        core.complete_skills_action(action, Ok(()));
        assert!(Arc::ptr_eq(
            &before,
            &core
                .skills_action_snapshot("workspace", &id.to_string())
                .unwrap()
        ));
        assert_plugins_quiet();
    }
    // NOT_RUN / NOT_COMPILED: phone archives must bind the existing uploader.
    #[test]
    fn native_plugin_preview_retains_origin_binding_and_never_auto_installs() {
        let (core, receiver) = fixture();
        let connection = core.provider_runtime_epoch().2.unwrap();
        let operation = core
            .start_skill_upload_bound(
                "workspace",
                SkillUploadTarget::PluginPreview,
                PathBuf::from("unused-phone-cache.zip"),
                Some(connection),
            )
            .unwrap();
        assert!(matches!(
            receiver.try_recv().map(|item| item.value),
            Ok(Work::Prepare { .. })
        ));
        core.complete_skill_preparation(
            "workspace",
            operation.id(),
            Ok((
                SkillUploadSourceKind::Plugin,
                SkillUploadArchive {
                    file_name: "plugin.zip".into(),
                    bytes: vec![1],
                    sha256: "digest".into(),
                    uncompressed_size_bytes: 1,
                },
            )),
        );
        assert!(matches!(
            core.next_skill_upload_effect("workspace", operation.id()),
            Some((
                UploadEffect::Start(_),
                SkillUploadTarget::PluginPreview,
                SkillUploadSourceKind::Plugin,
                1,
                true
            ))
        ));
        assert_eq!(
            core.skill_upload_connection("workspace", operation.id()),
            Some(connection)
        );
        assert!(
            core.start_skill_upload_bound(
                "workspace",
                SkillUploadTarget::PluginPreview,
                PathBuf::from("unused-phone-cache.zip"),
                Some(connection + 1)
            )
            .is_err()
        );
    }
    #[test]
    fn upload_progress_is_operation_only_and_drop_cancels_late_ack() {
        let (core, receiver) = fixture();
        let before = core.skills_catalog_snapshot("workspace").unwrap();
        let operation = core
            .start_skill_upload(
                "workspace",
                SkillUploadTarget::Install,
                PathBuf::from("synthetic-unused-picker-result"),
            )
            .unwrap();
        assert!(matches!(
            receiver.try_recv().map(|item| item.value),
            Ok(Work::Prepare { .. })
        ));
        core.complete_skill_preparation(
            "workspace",
            operation.id(),
            Ok((
                SkillUploadSourceKind::Skill,
                SkillUploadArchive {
                    file_name: "test.tar.gz".into(),
                    bytes: vec![1, 2],
                    sha256: "digest".into(),
                    uncompressed_size_bytes: 2,
                },
            )),
        );
        assert!(matches!(
            core.next_skill_upload_effect("workspace", operation.id()),
            Some((UploadEffect::Start(_), _, _, _, false))
        ));
        core.complete_skill_upload_effect(
            "workspace",
            operation.id(),
            1,
            Ok(UploadCompletion::Start(
                pioneer_protocol::SkillsUploadStartResponse {
                    upload_id: "remote".into(),
                    recommended_chunk_size_bytes: 1,
                    max_chunk_size_bytes: 1,
                    max_compressed_size_bytes: 8,
                    max_uncompressed_size_bytes: 8,
                    expires_at_unix: 1,
                },
            )),
        );
        assert!(matches!(
            core.next_skill_upload_effect("workspace", operation.id()),
            Some((UploadEffect::Chunk { offset: 0, .. }, _, _, _, false))
        ));
        core.complete_skill_upload_effect(
            "workspace",
            operation.id(),
            2,
            Ok(UploadCompletion::Chunk(
                pioneer_protocol::SkillsUploadChunkAckNotification {
                    upload_id: "remote".into(),
                    offset: 0,
                    len: 1,
                    received_bytes: 1,
                    next_offset: 1,
                },
            )),
        );
        assert_eq!(
            core.skill_upload_snapshot("workspace", operation.id())
                .unwrap()
                .sent_bytes,
            1
        );
        assert!(Arc::ptr_eq(
            &before,
            &core.skills_catalog_snapshot("workspace").unwrap()
        ));
        assert!(core.mcp_catalog_snapshot("workspace").is_none());
        let id = operation.id();
        drop(operation);
        let cancelled = core.skill_upload_snapshot("workspace", id).unwrap();
        assert_eq!(cancelled.state, SkillUploadState::Cancelled);
        assert!(matches!(
            receiver.try_recv().map(|item| item.value),
            Ok(Work::Abort { .. })
        ));
        core.complete_skill_upload_effect("workspace", id, 3, Ok(UploadCompletion::Applied));
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.skill_upload_snapshot("workspace", id).unwrap()
        ));
        assert!(core.next_skill_upload_effect("workspace", id).is_none());
        assert!(Arc::ptr_eq(
            &before,
            &core.skills_catalog_snapshot("workspace").unwrap()
        ));
    }
    #[test]
    fn upload_update_completes_once_and_stale_effect_errors_do_not_cancel_next_chunk() {
        let (core, receiver) = fixture();
        let (_plugins, assert_plugins_quiet) =
            crate::plugins::runtime::tests::watch_plugin_reads(&core);
        let before = core.skills_catalog_snapshot("workspace").unwrap();
        let id = skill('A').skill_id;
        let operation = core
            .start_skill_upload(
                "workspace",
                SkillUploadTarget::Update {
                    skill_id: id.clone(),
                },
                "synthetic-unused-picker-result".into(),
            )
            .unwrap();
        let op = operation.id();
        assert!(matches!(
            receiver.try_recv().map(|item| item.value),
            Ok(Work::Prepare { .. })
        ));
        core.complete_skill_preparation(
            "workspace",
            op,
            Ok((
                SkillUploadSourceKind::Skill,
                SkillUploadArchive {
                    file_name: "skill.tar.gz".into(),
                    bytes: vec![1, 2],
                    sha256: "digest".into(),
                    uncompressed_size_bytes: 2,
                },
            )),
        );
        assert!(
            matches!(core.next_skill_upload_effect("workspace",op),Some((UploadEffect::Start(_),SkillUploadTarget::Update{skill_id},_,1,false)) if skill_id==id)
        );
        core.complete_skill_upload_effect(
            "workspace",
            op,
            1,
            Ok(UploadCompletion::Start(
                pioneer_protocol::SkillsUploadStartResponse {
                    upload_id: "remote".into(),
                    recommended_chunk_size_bytes: 1,
                    max_chunk_size_bytes: 1,
                    max_compressed_size_bytes: 8,
                    max_uncompressed_size_bytes: 8,
                    expires_at_unix: 1,
                },
            )),
        );
        for (offset, ticket) in [(0, 2), (1, 3)] {
            assert!(
                matches!(core.next_skill_upload_effect("workspace",op),Some((UploadEffect::Chunk{offset:actual,..},_,_,generation,false)) if actual==offset&&generation==ticket)
            );
            let current = core.skill_upload_snapshot("workspace", op).unwrap();
            core.complete_skill_upload_effect(
                "workspace",
                op,
                ticket - 1,
                Err(anyhow::anyhow!("stale synthetic error")),
            );
            assert!(Arc::ptr_eq(
                &current,
                &core.skill_upload_snapshot("workspace", op).unwrap()
            ));
            core.complete_skill_upload_effect(
                "workspace",
                op,
                ticket,
                Ok(UploadCompletion::Chunk(
                    pioneer_protocol::SkillsUploadChunkAckNotification {
                        upload_id: "remote".into(),
                        offset,
                        len: 1,
                        received_bytes: offset + 1,
                        next_offset: offset + 1,
                    },
                )),
            );
        }
        assert!(matches!(
            core.next_skill_upload_effect("workspace", op),
            Some((UploadEffect::Finish(_), _, _, 4, false))
        ));
        core.complete_skill_upload_effect(
            "workspace",
            op,
            4,
            Ok(UploadCompletion::Finish(
                pioneer_protocol::SkillsUploadFinishResponse {
                    upload_id: "remote".into(),
                    status: "finalized".into(),
                    sha256: "digest".into(),
                    compressed_size_bytes: 2,
                },
            )),
        );
        assert!(matches!(
            core.next_skill_upload_effect("workspace", op),
            Some((
                UploadEffect::Apply { .. },
                SkillUploadTarget::Update { .. },
                _,
                5,
                false
            ))
        ));
        core.complete_skill_upload_effect("workspace", op, 5, Ok(UploadCompletion::Applied));
        let done = core.skill_upload_snapshot("workspace", op).unwrap();
        assert_eq!(done.state, SkillUploadState::Succeeded);
        core.complete_skill_upload_effect("workspace", op, 5, Ok(UploadCompletion::Applied));
        drop(operation);
        assert!(Arc::ptr_eq(
            &done,
            &core.skill_upload_snapshot("workspace", op).unwrap()
        ));
        assert!(Arc::ptr_eq(
            &before,
            &core.skills_catalog_snapshot("workspace").unwrap()
        ));
        assert!(receiver.try_recv().is_err());
        assert_plugins_quiet();
    }
    #[test]
    fn auto_install_rechecks_owner_after_preview_and_retains_dispatched_retry() {
        use crate::rpc::{JsonRpcRequestTransport, JsonRpcResponseError, JsonRpcResponseSender};
        use pioneer_protocol::{JsonRpcRequest, constants::methods};

        struct Replies(mpsc::Sender<(JsonRpcRequest, Option<JsonRpcResponseSender>)>);
        impl JsonRpcRequestTransport for Replies {
            fn send_json_rpc_request(
                &self,
                _: String,
                payload: String,
                reply: JsonRpcResponseSender,
            ) -> Result<(), String> {
                let request: JsonRpcRequest = serde_json::from_str(&payload).unwrap();
                let pending = if request.method == methods::PLUGINS_PREVIEW {
                    Some(reply)
                } else {
                    // A dispatched install has an unknown outcome, rather than
                    // a server rejection. Never block worker completion on it.
                    reply
                        .send(Err(JsonRpcResponseError::transport("lost response")))
                        .unwrap();
                    None
                };
                self.0
                    .send((request, pending))
                    .map_err(|error| error.to_string())
            }
        }

        for retire_during_preview in [false, true] {
            let (core, queued) = fixture();
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
            let (requests, received) = mpsc::channel();
            *core
                .transport_runtime()
                .ws_command_sender()
                .test_requests
                .lock()
                .unwrap() = Some(Arc::new(Replies(requests)));
            let source = tempfile::tempdir().unwrap();
            std::fs::write(source.path().join("asset.txt"), "package bytes").unwrap();
            let archive = build_plugin_upload_archive(source.path()).unwrap();
            let size = archive.bytes.len() as u64;
            let digest = archive.sha256.clone();
            let operation = core
                .start_skill_upload(
                    "workspace",
                    SkillUploadTarget::PluginInstall,
                    source.path().into(),
                )
                .unwrap();
            let work = queued.try_recv().unwrap();
            core.complete_skill_preparation(
                "workspace",
                operation.id(),
                Ok((SkillUploadSourceKind::Plugin, archive)),
            );
            // Finalize through the existing reducer, then run the actual worker
            // continuation. Binary chunk transport is covered at its own boundary.
            for completion in [
                UploadCompletion::Start(pioneer_protocol::SkillsUploadStartResponse {
                    upload_id: "same-upload".into(),
                    recommended_chunk_size_bytes: size,
                    max_chunk_size_bytes: size,
                    max_compressed_size_bytes: size,
                    max_uncompressed_size_bytes: 1024,
                    expires_at_unix: 60,
                }),
                UploadCompletion::Chunk(pioneer_protocol::SkillsUploadChunkAckNotification {
                    upload_id: "same-upload".into(),
                    offset: 0,
                    len: size,
                    received_bytes: size,
                    next_offset: size,
                }),
                UploadCompletion::Finish(pioneer_protocol::SkillsUploadFinishResponse {
                    upload_id: "same-upload".into(),
                    status: "finalized".into(),
                    sha256: digest,
                    compressed_size_bytes: size,
                }),
            ] {
                let (_, _, _, generation, _) = core
                    .next_skill_upload_effect("workspace", operation.id())
                    .unwrap();
                core.complete_skill_upload_effect(
                    "workspace",
                    operation.id(),
                    generation,
                    Ok(completion),
                );
            }
            assert_eq!(
                core.skill_upload_snapshot("workspace", operation.id())
                    .unwrap()
                    .state,
                SkillUploadState::Applying,
            );
            core.start_skills_operation_controller();
            let (sender, worker) = {
                let mut owner = core.capability_operations.lock().unwrap();
                (owner.sender.take().unwrap(), owner.task.take().unwrap())
            };
            assert!(sender.send(work).is_ok());
            drop(sender); // Actual thread join below proves no later install is queued.
            let (request, reply) = received
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            assert_eq!(request.method, methods::PLUGINS_PREVIEW);
            assert_eq!(request.params.as_ref().unwrap()["upload_id"], "same-upload");
            let reply = reply.unwrap();
            let epoch = core.provider_runtime_epoch();
            if retire_during_preview {
                let publication = core
                    .snapshot(&ClientScope::Administration { workspace_id: None })
                    .unwrap();
                let mut identity = (*publication
                    .snapshot()
                    .payload::<crate::gateway::identity_authorization::IdentityAuthorizationPublication>()
                    .unwrap())
                .clone();
                identity.authorization_change_sequence += 1;
                core.publish_identity_authorization(
                    &identity,
                    crate::gateway::identity_authorization::IdentityPublicationChange::Revalidate,
                );
                assert_ne!(core.provider_runtime_epoch(), epoch);
                assert_eq!(core.provider_runtime_epoch().2, epoch.2);
                assert!(core.capability_management_allowed("workspace"));
                assert!(
                    !core
                        .capability_operations
                        .lock()
                        .unwrap()
                        .uploads
                        .contains_key(&("workspace".into(), operation.id()))
                );
            }
            // The original socket/response is still live: rejection must come
            // from the retired operation, not a disconnected test transport.
            assert!(reply.is_active());
            let preview = pioneer_protocol::PluginsPreviewResponse {
                name: "Package".into(),
                version: None,
                description: None,
                fingerprint: "fingerprint".into(),
                components: vec![],
                diagnostics: vec![],
            };
            // Identity publishers retain this mutex while fencing the upload
            // owner. The live continuation must admit and dispatch using the
            // committed projection without waiting for that mutex in reverse.
            let identity =
                (!retire_during_preview).then(|| core.identity_authorization.lock().unwrap());
            reply
                .send(Ok(serde_json::to_value(preview).unwrap()))
                .unwrap();
            let install = if retire_during_preview {
                None
            } else {
                Some(received.recv_timeout(std::time::Duration::from_secs(5)))
            };
            drop(identity); // Release even on a failed watchdog before joining.
            worker.join().unwrap();
            if retire_during_preview {
                assert!(
                    received.try_recv().is_err(),
                    "retired preview dispatched another RPC",
                );
            } else {
                let (install, _) = install
                    .unwrap()
                    .expect("current preview must dispatch while identity is held");
                assert_eq!(install.method, methods::PLUGINS_INSTALL);
                let params = install.params.unwrap();
                assert_eq!(params["upload_id"], "same-upload");
                assert_eq!(params["expected_fingerprint"], "fingerprint");
                assert!(received.try_recv().is_err());
                let retry = core
                    .confirm_plugin_upload("workspace", operation.id(), epoch)
                    .unwrap();
                assert!(retry.mutation_uncertain);
                let retained = retry.plugin_preview.unwrap();
                assert_eq!(retained.upload_id, "same-upload");
                assert_eq!(retained.package.fingerprint, "fingerprint");
            }
        }
    }
    #[test]
    fn preview_survives_another_workspace_and_uncertain_confirmation_without_abort() {
        for auth_fence in [false, true] {
            let (core, receiver) = fixture();
            let operation = core
                .start_skill_upload(
                    "workspace",
                    SkillUploadTarget::PluginPreview,
                    "unused".into(),
                )
                .unwrap();
            let _ = receiver.try_recv().unwrap();
            core.complete_skill_preparation(
                "workspace",
                operation.id(),
                Ok((
                    SkillUploadSourceKind::Plugin,
                    SkillUploadArchive {
                        file_name: "plugin.tar.gz".into(),
                        bytes: vec![1],
                        sha256: "digest".into(),
                        uncompressed_size_bytes: 1,
                    },
                )),
            );
            let preview = pioneer_protocol::PluginsPreviewResponse {
                name: "Package".into(),
                version: None,
                description: None,
                fingerprint: "fingerprint".into(),
                components: vec![],
                diagnostics: vec![],
            };
            let completions = [
                UploadCompletion::Start(pioneer_protocol::SkillsUploadStartResponse {
                    upload_id: "same-upload".into(),
                    recommended_chunk_size_bytes: 1,
                    max_chunk_size_bytes: 1,
                    max_compressed_size_bytes: 8,
                    max_uncompressed_size_bytes: 8,
                    expires_at_unix: 60,
                }),
                UploadCompletion::Chunk(pioneer_protocol::SkillsUploadChunkAckNotification {
                    upload_id: "same-upload".into(),
                    offset: 0,
                    len: 1,
                    received_bytes: 1,
                    next_offset: 1,
                }),
                UploadCompletion::Finish(pioneer_protocol::SkillsUploadFinishResponse {
                    upload_id: "same-upload".into(),
                    status: "finalized".into(),
                    sha256: "digest".into(),
                    compressed_size_bytes: 1,
                }),
                UploadCompletion::PluginPreviewed("same-upload".into(), preview),
            ];
            for completion in completions {
                let (_, _, _, generation, _) = core
                    .next_skill_upload_effect("workspace", operation.id())
                    .unwrap();
                core.complete_skill_upload_effect(
                    "workspace",
                    operation.id(),
                    generation,
                    Ok(completion),
                );
            }
            if auth_fence {
                core.invalidate_skills_operations();
                let fenced = core
                    .skill_upload_snapshot("workspace", operation.id())
                    .unwrap();
                drop(operation);
                assert!(Arc::ptr_eq(
                    &fenced,
                    &core
                        .skill_upload_snapshot("workspace", fenced.operation_id)
                        .unwrap()
                ));
                continue;
            }
            let another = core
                .start_skill_upload(
                    "another-workspace",
                    SkillUploadTarget::PluginPreview,
                    "unused".into(),
                )
                .unwrap();
            let _ = receiver.try_recv().unwrap();
            let epoch = core.provider_runtime_epoch();
            let confirmed = core
                .confirm_plugin_upload("workspace", operation.id(), epoch)
                .unwrap();
            assert_eq!(
                confirmed.plugin_preview.as_ref().unwrap().upload_id,
                "same-upload"
            );
            core.cancel_skill_upload("workspace", operation.id());
            assert!(receiver.try_recv().is_err());
            core.complete_plugin_upload_confirmation("workspace", operation.id(), epoch, false);
            let id = operation.id();
            drop(operation);
            assert!(receiver.try_recv().is_err());
            let retry = core.confirm_plugin_upload("workspace", id, epoch).unwrap();
            assert!(retry.mutation_uncertain);
            assert_eq!(
                retry.plugin_preview.as_ref().unwrap().package.fingerprint,
                "fingerprint"
            );
            core.complete_plugin_upload_confirmation("workspace", id, epoch, true);
            let done = core.skill_upload_snapshot("workspace", id).unwrap();
            assert_eq!(done.state, SkillUploadState::Succeeded);
            assert!(done.plugin_preview.is_none() && !done.mutation_uncertain);
            drop(another);
        }
    }
}
