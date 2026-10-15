//! Skills mutations and upload execution use a bounded, process-local controller.
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
    Install,
    Update { skill_id: SkillId },
    UpdatePack { pack_id: SkillPackId },
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
pub(crate) struct SkillsController {
    pub(crate) fenced: bool,
    next: u64,
    publications: BTreeMap<(String, String), Arc<SkillsActionPublication>>,
    uploads: BTreeMap<(String, u64), Upload>,
    sender: Option<mpsc::SyncSender<Work>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl SkillsController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.publications.clear();
        self.uploads.clear();
    }
}
impl Drop for SkillsController {
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
    pub fn skills_intent(&self, workspace: &str, mut intent: SkillsIntent) -> anyhow::Result<u64> {
        anyhow::ensure!(
            self.capability_management_allowed(workspace),
            "capability_access_denied"
        );
        let epoch = self.provider_runtime_epoch();
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
            *allow_implicit_invocation = super::actions::effective_allow_implicit_invocation(
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
            .skills_controller
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
        owner.next = owner
            .next
            .checked_add(1)
            .expect("Skills operation identity exhausted");
        let operation = owner.next;
        owner
            .sender
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("client_stopped"))?
            .try_send(Work::Action(Action {
                workspace: workspace.into(),
                target: target.clone(),
                operation,
                epoch,
                intent: intent.clone(),
                previous_policy,
            }))
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
        owner: &mut SkillsController,
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
                    .skills_controller
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
    fn complete_skills_action(&self, action: Action, result: anyhow::Result<()>) {
        if !self.skills_action_current(&action) {
            return;
        }
        let mut owner = self
            .skills_controller
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
        if result.is_ok() {
            self.fence_skills_reads_after_action(&action.workspace);
        }
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
        anyhow::ensure!(
            self.capability_management_allowed(workspace),
            "capability_access_denied"
        );
        let epoch = self.provider_runtime_epoch();
        anyhow::ensure!(epoch.2.is_some(), "gateway_not_connected");
        if target != SkillUploadTarget::Install {
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
                SkillUploadTarget::Install => {}
            }
        }
        let mut owner = self
            .skills_controller
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
        owner
            .uploads
            .retain(|_, upload| !upload.flow.publication.state.is_terminal());
        owner.next = owner
            .next
            .checked_add(1)
            .expect("Skills operation identity exhausted");
        let operation = owner.next;
        owner
            .sender
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("client_stopped"))?
            .try_send(Work::Prepare {
                workspace: workspace.into(),
                operation,
                path,
            })
            .map_err(|_| anyhow::anyhow!("skills_upload_backpressure"))?;
        let mut upload = Upload {
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
            .skills_controller
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(upload) = owner.uploads.get_mut(&(workspace.into(), operation)) else {
            return;
        };
        let epoch = upload.epoch;
        upload.flow.terminate(SkillUploadState::Cancelled);
        let remote = upload.flow.take_cleanup();
        self.publish_skill_upload(workspace, upload);
        if let Some(remote) = remote
            && let Some(sender) = &owner.sender
        {
            let _ = sender.try_send(Work::Abort {
                workspace: workspace.into(),
                upload: remote,
                epoch,
            });
        }
    }
    pub(crate) fn invalidate_skills_operations(&self) {
        let mut owner = self
            .skills_controller
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
    }
    fn skill_preparation_target(
        &self,
        workspace: &str,
        operation: u64,
    ) -> Option<SkillUploadTarget> {
        let owner = self
            .skills_controller
            .lock()
            .expect("Skills controller poisoned");
        let upload = owner.uploads.get(&(workspace.into(), operation))?;
        self.upload_current(workspace, upload)
            .then(|| upload.target.clone())
    }
    fn complete_skill_preparation(
        &self,
        workspace: &str,
        operation: u64,
        prepared: anyhow::Result<(SkillUploadSourceKind, SkillUploadArchive)>,
    ) {
        let mut owner = self
            .skills_controller
            .lock()
            .expect("Skills controller poisoned");
        let Some(upload) = owner.uploads.get_mut(&(workspace.into(), operation)) else {
            return;
        };
        if !self.upload_current(workspace, upload)
            || upload.flow.publication.state != SkillUploadState::Preparing
        {
            return;
        }
        match prepared {
            Ok((kind, archive)) => {
                upload.source_kind = Some(kind);
                upload.flow.publication.pack = kind == SkillUploadSourceKind::Pack;
                upload.flow.prepare(archive);
            }
            Err(_) => {
                upload.flow.terminate(SkillUploadState::Failed);
            }
        }
        self.publish_skill_upload(workspace, upload);
    }
    fn next_skill_upload_effect(
        &self,
        workspace: &str,
        operation: u64,
    ) -> Option<(UploadEffect, SkillUploadTarget, SkillUploadSourceKind, u64)> {
        let mut owner = self
            .skills_controller
            .lock()
            .expect("Skills controller poisoned");
        let upload = owner.uploads.get_mut(&(workspace.into(), operation))?;
        if !self.upload_current(workspace, upload) {
            return None;
        }
        Some((
            upload.flow.effect()?,
            upload.target.clone(),
            upload.source_kind?,
            upload.flow.effect_generation(),
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
            .skills_controller
            .lock()
            .expect("Skills controller poisoned");
        let Some(upload) = owner.uploads.get_mut(&(workspace.into(), operation)) else {
            return;
        };
        if !self.upload_current(workspace, upload) {
            return;
        }
        if !upload.flow.accepts_effect(effect_generation) {
            return;
        }
        match result {
            Ok(completion) => {
                if !upload.flow.complete(effect_generation, completion) {
                    return;
                }
            }
            Err(_) => {
                upload.flow.terminate(SkillUploadState::Failed);
            }
        }
        self.publish_skill_upload(workspace, upload);
        let succeeded = upload.flow.publication.state == SkillUploadState::Succeeded;
        let cleanup = upload.flow.take_cleanup();
        let epoch = upload.epoch;
        if let Some(upload) = cleanup
            && let Some(sender) = &owner.sender
        {
            let _ = sender.try_send(Work::Abort {
                workspace: workspace.into(),
                upload,
                epoch,
            });
        }
        drop(owner);
        if succeeded {
            self.fence_skills_reads_after_action(workspace);
        }
    }
    pub(crate) fn start_skills_operation_controller(self: &Arc<Self>) {
        let (sender, receiver) = mpsc::sync_channel::<Work>(32);
        let weak = Arc::downgrade(self);
        let task=std::thread::Builder::new().name("client-skills-actions".into()).spawn(move||while let Ok(work)=receiver.recv(){
            match work {
                Work::Action(action)=>{
                    let Some(core)=weak.upgrade() else{return;};if !core.skills_action_current(&action){continue;}let sender=core.transport_runtime().ws_command_sender();drop(core);
                    let result=match &action.intent {
                        SkillsIntent::Policy{skill_id,enabled,allow_implicit_invocation}=>sender.skills_policy_set(super::actions::skills_policy_set_params(&action.workspace,skill_id.clone(),*enabled,*allow_implicit_invocation)).map(|_|()),
                        SkillsIntent::Remove{skill_id}=>sender.skills_uninstall(super::actions::skills_uninstall_params(&action.workspace,skill_id.clone())).map(|_|()),
                        SkillsIntent::RemovePack{pack_id}=>sender.skills_pack_uninstall(super::actions::skills_pack_uninstall_params(&action.workspace,pack_id.clone())).map(|_|()),
                    };if let Some(core)=weak.upgrade(){core.complete_skills_action(action,result);}
                }
                Work::Abort{workspace,upload,epoch}=>{
                    let Some(core)=weak.upgrade() else{return;};if core.is_stopped()||core.provider_runtime_epoch()!=epoch||!core.capability_management_allowed(&workspace){continue;}let sender=core.transport_runtime().ws_command_sender();drop(core);let _=sender.skills_upload_abort(super::upload::skills_upload_abort_params(workspace,upload));
                }
                Work::Prepare{workspace,operation,path}=>{
                    let Some(core)=weak.upgrade() else{return;};let target=core.skill_preparation_target(&workspace,operation);drop(core);let Some(target)=target else{continue;};
                    let prepared=prepare_archive_effect(target,path);
                    if let Some(core)=weak.upgrade(){core.complete_skill_preparation(&workspace,operation,prepared);}else{return;}

                    loop {
                        let Some(core)=weak.upgrade() else{return;};let effect=core.next_skill_upload_effect(&workspace,operation);let sender=core.transport_runtime().ws_command_sender();drop(core);let Some((effect,target,kind,effect_generation))=effect else{break;};
                        let result=match effect {
                            UploadEffect::Start(params)=>sender.skills_upload_start(params).map(UploadCompletion::Start),
                            UploadEffect::Chunk{upload_id,offset,bytes}=>sender.send_skill_upload_chunk(workspace.clone(),upload_id,offset,bytes).map(UploadCompletion::Chunk),
                            UploadEffect::Finish(params)=>sender.skills_upload_finish(params).map(UploadCompletion::Finish),
                            UploadEffect::Apply{upload_id}=>match target {
                                SkillUploadTarget::Install=>match kind {SkillUploadSourceKind::Skill=>sender.skills_install(super::actions::skills_install_uploaded_archive_params(&workspace,upload_id)).map(|_|()),SkillUploadSourceKind::Pack=>sender.skills_pack_install(super::actions::skills_pack_install_uploaded_archive_params(&workspace,upload_id)).map(|_|())},
                                SkillUploadTarget::Update{skill_id}=>sender.skills_update(super::actions::skills_update_uploaded_archive_params(&workspace,skill_id,upload_id,None)).map(|_|()),
                                SkillUploadTarget::UpdatePack{pack_id}=>sender.skills_pack_update(super::actions::skills_pack_update_uploaded_archive_params(&workspace,pack_id,upload_id)).map(|_|()),
                            }.map(|_|UploadCompletion::Applied),
                        };if let Some(core)=weak.upgrade(){core.complete_skill_upload_effect(&workspace,operation,effect_generation,result);}
                    }
                }
            }
        }).expect("Skills operation controller could not start");
        let mut owner = self
            .skills_controller
            .lock()
            .expect("Skills controller poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
}

/// Filesystem execution for the explicit directory selected by a shell. Tests inject archives.
fn prepare_archive_effect(
    target: SkillUploadTarget,
    path: PathBuf,
) -> anyhow::Result<(SkillUploadSourceKind, SkillUploadArchive)> {
    let kind = match target {
        SkillUploadTarget::Install => classify_skill_upload_source(&path)?,
        SkillUploadTarget::Update { .. } => SkillUploadSourceKind::Skill,
        SkillUploadTarget::UpdatePack { .. } => SkillUploadSourceKind::Pack,
    };
    let archive = match kind {
        SkillUploadSourceKind::Skill => build_skill_upload_archive(&path)?,
        SkillUploadSourceKind::Pack => build_skill_pack_upload_archive(&path)?,
    };
    Ok((kind, archive))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_test_support::{client, skill};
    fn fixture() -> (Arc<ClientCore>, mpsc::Receiver<Work>) {
        let core = client();
        core.accept_skills_catalog_for_test(
            "workspace",
            super::super::catalog::project_skills_snapshot(vec![skill('A'), skill('B')], vec![]),
        );
        let (sender, receiver) = mpsc::sync_channel(4);
        core.skills_controller.lock().unwrap().sender = Some(sender);
        (core, receiver)
    }
    #[test]
    fn skill_policy_remove_failure_retry_and_access_fence_share_one_controller() {
        let (core, receiver) = fixture();
        let id = skill('A').skill_id;
        let intent = || SkillsIntent::Policy {
            skill_id: id.clone(),
            enabled: false,
            allow_implicit_invocation: false,
        };
        let first = core.skills_intent("workspace", intent()).unwrap();
        assert!(core.skills_intent("workspace", intent()).is_err());
        let Work::Action(action) = receiver.try_recv().unwrap() else {
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
        let Work::Action(action) = receiver.try_recv().unwrap() else {
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
        let Work::Action(action) = receiver.try_recv().unwrap() else {
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
        assert!(matches!(receiver.try_recv(), Ok(Work::Prepare { .. })));
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
            Some((UploadEffect::Start(_), _, _, _))
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
            Some((UploadEffect::Chunk { offset: 0, .. }, _, _, _))
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
        assert!(matches!(receiver.try_recv(), Ok(Work::Abort { .. })));
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
        assert!(matches!(receiver.try_recv(), Ok(Work::Prepare { .. })));
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
            matches!(core.next_skill_upload_effect("workspace",op),Some((UploadEffect::Start(_),SkillUploadTarget::Update{skill_id},_,1)) if skill_id==id)
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
                matches!(core.next_skill_upload_effect("workspace",op),Some((UploadEffect::Chunk{offset:actual,..},_,_,generation)) if actual==offset&&generation==ticket)
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
            Some((UploadEffect::Finish(_), _, _, 4))
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
                5
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
    }
}
