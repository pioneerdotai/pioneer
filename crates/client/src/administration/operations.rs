//! Operation identity, permissions and completion ownership for administration.
use super::{AdministrationAction, pages::*};
use crate::{authorization::principal_presentation_capabilities, core::*};
use pioneer_protocol::*;
use std::sync::Arc;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", content = "params", rename_all = "snake_case")]
pub enum AdministrationCommand {
    CreateInvitation(InvitationCreateParams),
    RevokeInvitation(InvitationRevokeParams),
    SuspendMember(MemberSuspendParams),
    RestoreMember(MemberRestoreParams),
    RemoveMember(MemberRemoveParams),
    CreateRecoveryDevice(MemberDeviceCreateParams),
    AddWorkspaceMember(WorkspaceMemberAddParams),
    RemoveWorkspaceMember(WorkspaceMemberRemoveParams),
    SetMemberWorkspaces {
        principal_id: PrincipalId,
        selected: Vec<WorkspaceId>,
    },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AdministrationPresentationIntent {
    CopyActivation { generation: u64 },
    DismissActivation { generation: u64 },
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind")]
pub enum AdministrationPresentationEffect {
    #[serde(rename = "copy_administration_activation")]
    CopyActivation,
}
// Secret-bearing outputs belong only to the executing operation/caller. They
// never enter a publication, change set, persistent cache or diagnostic.
pub enum AdministrationCompletion {
    InvitationCreated(InvitationCreateResponse),
    InvitationRevoked(InvitationRevokeResponse),
    MemberChanged(MemberMutationResponse),
    RecoveryDeviceCreated(MemberDeviceCreateResponse),
    WorkspaceMemberChanged(WorkspaceMemberMutationResponse),
    MemberWorkspacesChanged,
}
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct AdministrationOperationPublication {
    pub revision: u64,
    pub generation: u64,
    pub action: Option<AdministrationAction>,
    pub request: AdministrationLoadState,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Operation {
    generation: u64,
    epoch: (u64, u64, Option<u64>),
    action: AdministrationAction,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdministrationActivationKind {
    Invitation,
    RecoveryDevice,
}

pub struct AdministrationOperation {
    identity: Operation,
    command: AdministrationCommand,
}
struct ActivationCleanup {
    epoch: (u64, u64, Option<u64>),
    session_id: AuthSessionId,
}
enum AdministrationWork {
    Execute {
        operation: AdministrationOperation,
        reply: Option<std::sync::mpsc::SyncSender<anyhow::Result<AdministrationCompletion>>>,
    },
    Wake,
}
#[derive(Default)]
pub(crate) struct AdministrationOperationController {
    generation: u64,
    presentation_generation: u64,
    active: Option<Operation>,
    prepared: Option<AdministrationOperation>,
    publication: Option<Arc<AdministrationOperationPublication>>,
    recovery: Option<(u64, ActivationCleanup)>,
    cleanup: Option<ActivationCleanup>,
    sender: Option<std::sync::mpsc::SyncSender<AdministrationWork>>,
    task: Option<std::thread::JoinHandle<()>>,
}
impl AdministrationOperationController {
    pub(crate) fn stop(&mut self) {
        self.sender.take();
        self.active = None;
        self.prepared = None;
        self.recovery = None;
        self.cleanup = None;
    }
}
impl Drop for AdministrationOperationController {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.task.take() {
            if task.thread().id() != std::thread::current().id() {
                let _ = task.join();
            }
        }
    }
}
impl AdministrationCommand {
    fn action(&self) -> AdministrationAction {
        match self {
            Self::CreateInvitation(_) => AdministrationAction::CreateInvitation,
            Self::RevokeInvitation(p) => AdministrationAction::RevokeInvitation {
                invitation_id: p.invitation_id.clone(),
            },
            Self::SuspendMember(p) => AdministrationAction::SuspendMember {
                principal_id: p.principal_id.clone(),
            },
            Self::RestoreMember(p) => AdministrationAction::RestoreMember {
                principal_id: p.principal_id.clone(),
            },
            Self::RemoveMember(p) => AdministrationAction::RemoveMember {
                principal_id: p.principal_id.clone(),
            },
            Self::CreateRecoveryDevice(p) => AdministrationAction::CreateRecoveryDevice {
                principal_id: p.principal_id.clone(),
            },
            Self::AddWorkspaceMember(p) => AdministrationAction::AddWorkspaceMember {
                principal_id: p.principal_id.clone(),
                workspace_id: p.workspace_id.clone(),
            },
            Self::RemoveWorkspaceMember(p) => AdministrationAction::RemoveWorkspaceMember {
                principal_id: p.principal_id.clone(),
                workspace_id: p.workspace_id.clone(),
            },
            Self::SetMemberWorkspaces { principal_id, .. } => {
                AdministrationAction::SetMemberWorkspaces {
                    principal_id: principal_id.clone(),
                }
            }
        }
    }
}
impl ClientCore {
    pub fn administration_presentation_intent(
        &self,
        intent: AdministrationPresentationIntent,
    ) -> ClientTransition {
        let generation = match intent {
            AdministrationPresentationIntent::CopyActivation { generation }
            | AdministrationPresentationIntent::DismissActivation { generation } => generation,
        };
        let mut owner = self
            .administration_operations
            .lock()
            .expect("administration operations poisoned");
        let Some(publication) = owner.publication.clone().filter(|p| {
            p.generation == generation
                && matches!(
                    p.action,
                    Some(
                        AdministrationAction::CreateInvitation
                            | AdministrationAction::CreateRecoveryDevice { .. }
                    )
                )
        }) else {
            return self.reject_intent();
        };
        if self.is_stopped() {
            return self.reject_intent();
        }
        if matches!(
            intent,
            AdministrationPresentationIntent::DismissActivation { .. }
        ) {
            if publication.request == AdministrationLoadState::Cancelled {
                return self.navigation_outcome(ClientTransitionOutcome::Noop);
            }
            owner.active = None;
            owner.prepared = None;
            if let Some((_, cleanup)) = owner.recovery.take() {
                owner.cleanup = Some(cleanup);
                if let Some(sender) = &owner.sender {
                    let _ = sender.try_send(AdministrationWork::Wake);
                }
            }
            let operation = Operation {
                generation,
                epoch: self.administration_epoch(),
                action: publication.action.clone().unwrap(),
            };
            self.publish_administration_operation(
                &mut owner,
                &operation,
                AdministrationLoadState::Cancelled,
            );
            return self.navigation_outcome(ClientTransitionOutcome::Changed);
        }
        if publication.request != AdministrationLoadState::Ready {
            return self.reject_intent();
        }
        owner.presentation_generation = owner
            .presentation_generation
            .checked_add(1)
            .expect("administration presentation generation exhausted");
        let effect = ClientEffectPlan::new(
            ClientOperationId::new("administration/activation/copy").unwrap(),
            ClientGeneration::new(owner.presentation_generation),
            ClientPlannedEffect::AdministrationPresentation(
                AdministrationPresentationEffect::CopyActivation,
            ),
        );
        self.transition(
            &ClientMutationAuthority { _private: () },
            vec![],
            vec![effect],
        )
    }
    pub fn administration_recovery_presentation(
        &self,
        generation: u64,
        response: MemberDeviceCreateResponse,
    ) -> anyhow::Result<crate::gateway::device_activation::DeviceActivationQrPresentation> {
        anyhow::ensure!(
            self.administration_operation_snapshot().is_some_and(
                |p| p.generation == generation && p.request == AdministrationLoadState::Ready
            ),
            "administration_activation_unavailable"
        );
        let access = self
            .compatibility_runtime()
            .ws_command_sender()
            .current_gateway_http_access()
            .map_err(|_| anyhow::anyhow!("administration_activation_unavailable"))?;
        crate::gateway::device_activation::DeviceActivationQrPresentation::from_created_device(
            &access.gateway_base_url,
            response.activation,
        )
    }
    pub fn administration_operation_snapshot(
        &self,
    ) -> Option<Arc<AdministrationOperationPublication>> {
        self.snapshot(&ClientScope::AdministrationOperation)
            .and_then(|p| p.snapshot().payload())
    }
    fn publish_administration_operation(
        &self,
        owner: &mut AdministrationOperationController,
        operation: &Operation,
        request: AdministrationLoadState,
    ) {
        let scope = ClientScope::AdministrationOperation;
        let revision = self
            .snapshot(&scope)
            .map_or(0, |p| p.revisions().scoped().get())
            .checked_add(1)
            .expect("administration operation revision exhausted");
        let next = Arc::new(AdministrationOperationPublication {
            revision,
            generation: operation.generation,
            action: Some(operation.action.clone()),
            request,
        });
        owner.publication = Some(next.clone());
        self.publish(
            &ClientMutationAuthority { _private: () },
            scope,
            crate::threads::registry::revisions(revision),
            next,
            vec![],
        );
    }
    fn administration_command_allowed(&self, command: &AdministrationCommand) -> bool {
        let workspace = match command {
            AdministrationCommand::AddWorkspaceMember(p) => Some(p.workspace_id.as_str()),
            AdministrationCommand::RemoveWorkspaceMember(p) => Some(p.workspace_id.as_str()),
            _ => None,
        };
        let Some(snapshot) = self
            .authorization_snapshot(workspace, None)
            .or_else(|| self.authorization_snapshot(None, None))
        else {
            return false;
        };
        let capabilities = principal_presentation_capabilities(&snapshot);
        match command {
            AdministrationCommand::CreateInvitation(params) => {
                capabilities.can_create_invitation
                    && snapshot
                        .global
                        .invitation_role_options
                        .iter()
                        .any(|option| option.role.key == params.role_key.as_str())
            }
            AdministrationCommand::RevokeInvitation(params) => self
                .administration_page_snapshot(&AdministrationPage::Invitations)
                .is_some_and(|page| {
                    page.invitations.iter().any(|row| {
                        row.id == params.invitation_id
                            && super::invitation_list_row(&row.invitation, capabilities).can_revoke
                    })
                }),
            _ => {
                let action = command.action();
                let principal_id = match &action {
                    AdministrationAction::SuspendMember { principal_id }
                    | AdministrationAction::RestoreMember { principal_id }
                    | AdministrationAction::RemoveMember { principal_id }
                    | AdministrationAction::CreateRecoveryDevice { principal_id }
                    | AdministrationAction::AddWorkspaceMember { principal_id, .. }
                    | AdministrationAction::RemoveWorkspaceMember { principal_id, .. }
                    | AdministrationAction::SetMemberWorkspaces { principal_id } => principal_id,
                    _ => return false,
                };
                let Some(member) = self
                    .administration_page_snapshot(&AdministrationPage::Members)
                    .filter(|p| p.members.iter().any(|row| &row.id == principal_id))
                    .or_else(|| {
                        self.administration_page_snapshot(&AdministrationPage::MemberDirectory)
                    })
                    .and_then(|p| {
                        p.members
                            .iter()
                            .find(|row| &row.id == principal_id)
                            .map(|r| r.member.clone())
                    })
                else {
                    return false;
                };
                let is_workspace_member = workspace
                    .and_then(|id| WorkspaceId::new(id.to_owned()).ok())
                    .and_then(|workspace_id| {
                        self.administration_page_snapshot(&AdministrationPage::WorkspaceMembers {
                            workspace_id,
                        })
                    })
                    .is_some_and(|page| page.members.iter().any(|row| &row.id == principal_id));
                let actions = super::member_list_row(
                    &member,
                    Some(&snapshot.principal_id),
                    capabilities,
                    is_workspace_member,
                )
                .actions;
                match command {
                    AdministrationCommand::SuspendMember(_) => actions.can_suspend,
                    AdministrationCommand::RestoreMember(_) => actions.can_restore,
                    AdministrationCommand::RemoveMember(_) => actions.can_remove,
                    AdministrationCommand::CreateRecoveryDevice(_) => {
                        actions.can_create_recovery_device
                    }
                    AdministrationCommand::AddWorkspaceMember(_) => actions.can_add_to_workspace,
                    AdministrationCommand::RemoveWorkspaceMember(_) => {
                        actions.can_remove_from_workspace
                    }
                    AdministrationCommand::SetMemberWorkspaces { .. } => {
                        capabilities.can_add_workspace_member
                            || capabilities.can_remove_workspace_member
                    }
                    _ => false,
                }
            }
        }
    }
    /// Executes on a caller's background executor. Claiming and completing the
    /// typed command remain serialized in Client; no shell owns retry or reducers.
    pub fn prepare_administration_command(
        &self,
        command: AdministrationCommand,
    ) -> anyhow::Result<AdministrationOperation> {
        anyhow::ensure!(
            !self.is_stopped() && self.administration_command_allowed(&command),
            "administration_action_forbidden"
        );
        let epoch = self.administration_epoch();
        let operation = {
            let mut owner = self
                .administration_operations
                .lock()
                .expect("administration operations poisoned");
            anyhow::ensure!(owner.active.is_none(), "administration_action_pending");
            if let Some((_, cleanup)) = owner.recovery.take() {
                owner.cleanup = Some(cleanup);
                if let Some(sender) = &owner.sender {
                    let _ = sender.try_send(AdministrationWork::Wake);
                }
            }
            owner.generation = owner
                .generation
                .checked_add(1)
                .expect("administration operation generation exhausted");
            let operation = Operation {
                generation: owner.generation,
                epoch,
                action: command.action(),
            };
            owner.active = Some(operation.clone());
            self.publish_administration_operation(
                &mut owner,
                &operation,
                AdministrationLoadState::Loading,
            );
            operation
        };
        Ok(AdministrationOperation {
            identity: operation,
            command,
        })
    }
    pub fn execute_administration_command(
        self: &Arc<Self>,
        command: AdministrationCommand,
    ) -> anyhow::Result<AdministrationCompletion> {
        self.prepare_administration_command(command)?
            .execute(Arc::downgrade(self))
    }
    pub fn execute_prepared_administration_command(
        self: &Arc<Self>,
        operation: AdministrationOperation,
    ) -> anyhow::Result<AdministrationCompletion> {
        operation.execute(Arc::downgrade(self))
    }
    fn administration_operation_current(&self, operation: &Operation) -> bool {
        !self.is_stopped()
            && self.administration_epoch() == operation.epoch
            && self
                .administration_operations
                .lock()
                .expect("administration operations poisoned")
                .active
                .as_ref()
                == Some(operation)
    }
    fn plan_member_workspace_selection(
        &self,
        principal_id: PrincipalId,
        selected: Vec<WorkspaceId>,
    ) -> anyhow::Result<Vec<AdministrationCommand>> {
        let pages = self.administration_workspace_pages();
        let selected: std::collections::BTreeSet<_> = selected.into_iter().collect();
        let catalog = self.workspace_catalog();
        let catalog_ids: std::collections::BTreeSet<_> = catalog
            .workspaces()
            .iter()
            .map(|workspace| WorkspaceId::new(workspace.id.clone()))
            .collect::<Result<_, _>>()?;
        anyhow::ensure!(
            selected.is_subset(&catalog_ids),
            "workspace_members_unavailable"
        );
        anyhow::ensure!(catalog_ids.iter().all(|id| pages.iter().any(|page| matches!(&page.page, AdministrationPage::WorkspaceMembers { workspace_id } if workspace_id == id) && page.request == AdministrationLoadState::Ready && page.next_cursor.is_none())), "workspace_members_unavailable");
        let mut commands = Vec::new();
        for page in pages {
            let AdministrationPage::WorkspaceMembers { workspace_id } = &page.page else {
                continue;
            };
            if !catalog_ids.contains(workspace_id) {
                continue;
            }
            let included = page.members.iter().any(|row| row.id == principal_id);
            if included == selected.contains(workspace_id) {
                continue;
            }
            commands.push(if included {
                AdministrationCommand::RemoveWorkspaceMember(WorkspaceMemberRemoveParams {
                    workspace_id: workspace_id.clone(),
                    principal_id: principal_id.clone(),
                })
            } else {
                AdministrationCommand::AddWorkspaceMember(WorkspaceMemberAddParams {
                    workspace_id: workspace_id.clone(),
                    principal_id: principal_id.clone(),
                })
            });
        }
        Ok(commands)
    }
    fn finish_administration_command(
        &self,
        operation: &Operation,
        result: anyhow::Result<AdministrationCompletion>,
    ) -> anyhow::Result<AdministrationCompletion> {
        let epoch = self.administration_epoch();
        let mut owner = self
            .administration_operations
            .lock()
            .expect("administration operations poisoned");
        let current = !self.is_stopped()
            && epoch == operation.epoch
            && owner.active.as_ref() == Some(operation);
        if let Ok(AdministrationCompletion::RecoveryDeviceCreated(response)) = &result {
            if !self.is_stopped() && epoch == operation.epoch {
                let cleanup = ActivationCleanup {
                    epoch,
                    session_id: response.activation.session_id.clone(),
                };
                if current {
                    owner.recovery = Some((operation.generation, cleanup));
                } else {
                    owner.cleanup = Some(cleanup);
                    if let Some(sender) = &owner.sender {
                        let _ = sender.try_send(AdministrationWork::Wake);
                    }
                }
            }
        }
        anyhow::ensure!(current, "administration_action_cancelled");
        owner.active = None;
        owner.prepared = None;
        self.publish_administration_operation(
            &mut owner,
            operation,
            if result.is_ok() {
                AdministrationLoadState::Ready
            } else {
                AdministrationLoadState::Failed
            },
        );
        drop(owner);
        for target in super::conflict_refetch(&operation.action) {
            let page = match target {
                super::AdministrationRefetch::InvitationList => AdministrationPage::Invitations,
                super::AdministrationRefetch::MemberDirectory => AdministrationPage::Members,
                super::AdministrationRefetch::WorkspaceMembers { workspace_id } => {
                    AdministrationPage::WorkspaceMembers { workspace_id }
                }
            };
            self.administration_page_intent(AdministrationPageIntent::Refresh { page });
        }
        if matches!(
            operation.action,
            AdministrationAction::SetMemberWorkspaces { .. }
        ) {
            self.refresh_administration_member_pages();
        }
        result
    }
    pub fn administration_command_intent(
        &self,
        command: AdministrationCommand,
    ) -> ClientTransition {
        let prepared = match self.prepare_administration_command(command) {
            Ok(prepared) => prepared,
            Err(_) => return self.reject_intent(),
        };
        if matches!(
            prepared.command,
            AdministrationCommand::CreateInvitation(_)
                | AdministrationCommand::CreateRecoveryDevice(_)
        ) {
            let mut owner = self
                .administration_operations
                .lock()
                .expect("administration operations poisoned");
            if owner.active.as_ref() != Some(&prepared.identity) {
                return self.reject_intent();
            }
            owner.prepared = Some(prepared);
            return self.navigation_outcome(ClientTransitionOutcome::Changed);
        }
        let identity = prepared.identity.clone();
        let queued = self
            .administration_operations
            .lock()
            .expect("administration operations poisoned")
            .sender
            .as_ref()
            .is_some_and(|sender| {
                sender
                    .try_send(AdministrationWork::Execute {
                        operation: prepared,
                        reply: None,
                    })
                    .is_ok()
            });
        if !queued {
            let _ = self.finish_administration_command(
                &identity,
                Err(anyhow::anyhow!("administration_queue_unavailable")),
            );
        }
        self.navigation_outcome(ClientTransitionOutcome::Changed)
    }
    pub(crate) fn start_administration_operation_controller(self: &Arc<Self>) {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<AdministrationWork>(1);
        let weak = Arc::downgrade(self);
        let task = std::thread::Builder::new()
            .name("client-administration-actions".into())
            .spawn(move || {
                while let Ok(work) = receiver.recv() {
                    let Some(core) = weak.upgrade() else {
                        return;
                    };
                    let cleanup = core
                        .administration_operations
                        .lock()
                        .expect("administration operations poisoned")
                        .cleanup
                        .take();
                    if let Some(cleanup) = cleanup {
                        if !core.is_stopped() && core.administration_epoch() == cleanup.epoch {
                            let _ = core.revoke_auth_session(AuthSessionRevokeParams {
                                session_id: cleanup.session_id,
                                expected_status: Some(AuthSessionStatus::Pending),
                            });
                        }
                    }
                    drop(core);
                    if let AdministrationWork::Execute { operation, reply } = work {
                        let result = operation.execute_direct(weak.clone());
                        if let Some(reply) = reply {
                            let _ = reply.try_send(result);
                        }
                    }
                }
            })
            .expect("administration action worker");
        let mut owner = self
            .administration_operations
            .lock()
            .expect("administration operations poisoned");
        owner.sender = Some(sender);
        owner.task = Some(task);
    }
    pub fn take_administration_activation_operation(
        &self,
        generation: u64,
        kind: AdministrationActivationKind,
    ) -> anyhow::Result<AdministrationOperation> {
        let epoch = self.administration_epoch();
        let mut owner = self
            .administration_operations
            .lock()
            .expect("administration operations poisoned");
        anyhow::ensure!(
            !self.is_stopped()
                && owner
                    .active
                    .as_ref()
                    .is_some_and(|active| active.generation == generation && active.epoch == epoch),
            "administration_action_cancelled"
        );
        anyhow::ensure!(
            owner.prepared.as_ref().is_some_and(|prepared| matches!(
                (&prepared.command, kind),
                (
                    AdministrationCommand::CreateInvitation(_),
                    AdministrationActivationKind::Invitation
                ) | (
                    AdministrationCommand::CreateRecoveryDevice(_),
                    AdministrationActivationKind::RecoveryDevice
                )
            )),
            "administration_activation_mismatch"
        );
        owner
            .prepared
            .take()
            .ok_or_else(|| anyhow::anyhow!("administration_activation_already_claimed"))
    }
    pub(crate) fn cancel_administration_page_operations(&self, page: &AdministrationPage) {
        if matches!(
            page,
            AdministrationPage::WorkspaceMembers { .. } | AdministrationPage::MemberDirectory
        ) {
            return;
        }
        let mut owner = self
            .administration_operations
            .lock()
            .expect("administration operations poisoned");
        let Some(operation) = owner.active.clone().or_else(|| {
            owner
                .publication
                .as_ref()
                .filter(|p| {
                    p.request == AdministrationLoadState::Ready
                        && matches!(
                            p.action,
                            Some(
                                AdministrationAction::CreateInvitation
                                    | AdministrationAction::CreateRecoveryDevice { .. }
                            )
                        )
                })
                .and_then(|p| {
                    p.action.clone().map(|action| Operation {
                        generation: p.generation,
                        epoch: self.administration_epoch(),
                        action,
                    })
                })
        }) else {
            return;
        };
        let invitation = matches!(
            operation.action,
            AdministrationAction::CreateInvitation | AdministrationAction::RevokeInvitation { .. }
        );
        if invitation != matches!(page, AdministrationPage::Invitations) {
            return;
        }
        owner.active = None;
        owner.prepared = None;
        if let Some((_, cleanup)) = owner.recovery.take() {
            owner.cleanup = Some(cleanup);
            if let Some(sender) = &owner.sender {
                let _ = sender.try_send(AdministrationWork::Wake);
            }
        }
        self.publish_administration_operation(
            &mut owner,
            &operation,
            AdministrationLoadState::Cancelled,
        );
    }
    pub(crate) fn invalidate_administration_operations(&self) {
        let mut owner = self
            .administration_operations
            .lock()
            .expect("administration operations poisoned");
        owner.active = None;
        owner.prepared = None;
        owner.publication = None;
        owner.recovery = None;
        owner.cleanup = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> ClientCore {
        let core = ClientCore::new();
        let role = AuthorizationRolePresentation {
            key: "admin".into(),
            display_name: "Administrator".into(),
            description: String::new(),
            built_in: false,
        };
        assert_eq!(
            core.accept_authorization_projection(
                0,
                None,
                AuthorizationCapabilitySnapshot {
                    schema_version: AUTHORIZATION_CAPABILITY_SNAPSHOT_SCHEMA_VERSION,
                    authorization_revision: 1,
                    principal_id: PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").unwrap(),
                    role_key: "admin".into(),
                    role: role.clone(),
                    global: AuthorizationGlobalCapabilities {
                        can_view_member_directory: true,
                        can_view_invitations: true,
                        can_create_invitation: true,
                        can_manage_member_lifecycle: true,
                        invitation_role_options: vec![AuthorizationInvitationRoleOption {
                            role,
                            is_default: true
                        }],
                        ..Default::default()
                    },
                    workspace: None,
                    thread: None,
                }
            ),
            crate::authorization::AuthorizationProjectionAcceptance::Accepted
        );
        let member: MemberSummary = serde_json::from_value(serde_json::json!({"principal_id":"PBBBBBBBBBBBBBBBBBBBB","kind":"user","display_name":"Synthetic member","nickname":"member","role":{"key":"member","display_name":"Member","description":"","built_in":false},"lifecycle_managed":true,"status":"active"})).unwrap();
        core.complete_administration_members_for_test(vec![member]);
        core
    }
    fn remove() -> AdministrationCommand {
        AdministrationCommand::RemoveMember(MemberRemoveParams {
            principal_id: PrincipalId::new("PBBBBBBBBBBBBBBBBBBBB").unwrap(),
            expected_status: Some(PrincipalStatus::Active),
        })
    }
    fn invite() -> AdministrationCommand {
        AdministrationCommand::CreateInvitation(
            InvitationCreateParams::new_for_role(
                RoleKey::new("admin").unwrap(),
                vec![WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA").unwrap()],
            )
            .unwrap(),
        )
    }
    #[test]
    fn dismiss_activation_is_generation_scoped_and_prevents_late_claim_and_copy() {
        let core = fixture();
        core.administration_command_intent(invite());
        let loading = core.administration_operation_snapshot().unwrap();
        let generation = loading.generation;
        let wrong = core.administration_presentation_intent(
            AdministrationPresentationIntent::DismissActivation {
                generation: generation + 1,
            },
        );
        assert_eq!(wrong.outcome(), ClientTransitionOutcome::Rejected);
        assert!(Arc::ptr_eq(
            &loading,
            &core.administration_operation_snapshot().unwrap()
        ));
        assert!(
            core.administration_presentation_intent(
                AdministrationPresentationIntent::CopyActivation { generation }
            )
            .effects()
            .is_empty()
        );
        core.administration_presentation_intent(
            AdministrationPresentationIntent::DismissActivation { generation },
        );
        let cancelled = core.administration_operation_snapshot().unwrap();
        assert_eq!(cancelled.request, AdministrationLoadState::Cancelled);
        assert!(
            core.take_administration_activation_operation(
                generation,
                AdministrationActivationKind::Invitation
            )
            .is_err()
        );
        core.administration_presentation_intent(
            AdministrationPresentationIntent::DismissActivation { generation },
        );
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.administration_operation_snapshot().unwrap()
        ));
        core.administration_command_intent(invite());
        let retry = core.administration_operation_snapshot().unwrap();
        assert!(retry.generation > generation);
        core.administration_presentation_intent(
            AdministrationPresentationIntent::DismissActivation { generation },
        );
        assert!(Arc::ptr_eq(
            &retry,
            &core.administration_operation_snapshot().unwrap()
        ));
    }
    #[test]
    fn action_claim_is_single_and_failure_duplicate_or_cancelled_completion_cannot_publish() {
        let core = fixture();
        let operation = core.prepare_administration_command(remove()).unwrap();
        assert!(core.prepare_administration_command(remove()).is_err());
        assert!(
            core.finish_administration_command(
                &operation.identity,
                Err(anyhow::anyhow!("synthetic rejection"))
            )
            .is_err()
        );
        let failed = core.administration_operation_snapshot().unwrap();
        assert_eq!(failed.request, AdministrationLoadState::Failed);
        assert!(
            core.finish_administration_command(
                &operation.identity,
                Ok(AdministrationCompletion::MemberWorkspacesChanged)
            )
            .is_err()
        );
        assert!(Arc::ptr_eq(
            &failed,
            &core.administration_operation_snapshot().unwrap()
        ));
        let retry = core.prepare_administration_command(remove()).unwrap();
        assert!(retry.identity.generation > operation.identity.generation);
        core.cancel_administration_page_operations(&AdministrationPage::Members);
        let cancelled = core.administration_operation_snapshot().unwrap();
        assert_eq!(cancelled.request, AdministrationLoadState::Cancelled);
        assert!(
            core.finish_administration_command(
                &retry.identity,
                Ok(AdministrationCompletion::MemberWorkspacesChanged)
            )
            .is_err()
        );
        assert!(Arc::ptr_eq(
            &cancelled,
            &core.administration_operation_snapshot().unwrap()
        ));
    }
    #[test]
    fn transient_activation_requires_matching_generation_kind_and_exactly_one_claim() {
        let core = fixture();
        core.administration_command_intent(invite());
        let publication = core.administration_operation_snapshot().unwrap();
        let generation = publication.generation;
        assert!(
            core.take_administration_activation_operation(
                generation + 1,
                AdministrationActivationKind::Invitation
            )
            .is_err()
        );
        assert!(
            core.take_administration_activation_operation(
                generation,
                AdministrationActivationKind::RecoveryDevice
            )
            .is_err()
        );
        assert!(
            core.take_administration_activation_operation(
                generation,
                AdministrationActivationKind::Invitation
            )
            .is_ok()
        );
        assert!(
            core.take_administration_activation_operation(
                generation,
                AdministrationActivationKind::Invitation
            )
            .is_err()
        );
        let json = serde_json::to_value(publication).unwrap();
        let keys: std::collections::BTreeSet<_> = json
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(
            keys,
            ["action", "generation", "request", "revision"]
                .into_iter()
                .collect()
        );
        assert_eq!(
            json["action"],
            serde_json::json!({"kind":"create_invitation"})
        );
    }
    #[test]
    fn workspace_subscription_release_does_not_cancel_a_members_action() {
        let core = fixture();
        let operation = core.prepare_administration_command(remove()).unwrap();
        let current = core.administration_operation_snapshot().unwrap();
        core.cancel_administration_page_operations(&AdministrationPage::WorkspaceMembers {
            workspace_id: WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA").unwrap(),
        });
        assert!(Arc::ptr_eq(
            &current,
            &core.administration_operation_snapshot().unwrap()
        ));
        assert!(
            core.finish_administration_command(
                &operation.identity,
                Ok(AdministrationCompletion::MemberWorkspacesChanged)
            )
            .is_ok()
        );
    }
    #[test]
    fn permission_revocation_prevents_claims_and_late_success() {
        let core = fixture();
        let operation = core.prepare_administration_command(remove()).unwrap();
        core.invalidate_authorization_revision(2);
        assert!(core.prepare_administration_command(remove()).is_err());
        assert!(
            core.finish_administration_command(
                &operation.identity,
                Ok(AdministrationCompletion::MemberWorkspacesChanged)
            )
            .is_err()
        );
        assert!(
            core.administration_page_snapshot(&AdministrationPage::Members)
                .is_none()
        );
        assert!(core.administration_operation_snapshot().is_none());
    }
}

impl AdministrationOperation {
    /// Runs on a background executor without keeping a dropped Client alive
    /// during transport I/O. Every step rechecks the original operation.
    pub fn execute(
        self,
        client: std::sync::Weak<ClientCore>,
    ) -> anyhow::Result<AdministrationCompletion> {
        let core = client
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("administration_action_cancelled"))?;
        let identity = self.identity.clone();
        anyhow::ensure!(
            core.administration_operation_current(&identity),
            "administration_action_cancelled"
        );
        let (reply, receiver) = std::sync::mpsc::sync_channel(1);
        let queued = core
            .administration_operations
            .lock()
            .expect("administration operations poisoned")
            .sender
            .as_ref()
            .is_some_and(|sender| {
                sender
                    .try_send(AdministrationWork::Execute {
                        operation: self,
                        reply: Some(reply),
                    })
                    .is_ok()
            });
        if !queued {
            return core.finish_administration_command(
                &identity,
                Err(anyhow::anyhow!("administration_queue_unavailable")),
            );
        }
        drop(core);
        loop {
            match receiver.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(result) => return result,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    anyhow::ensure!(
                        client.upgrade().is_some_and(|core| !core.is_stopped()
                            && core.administration_epoch() == identity.epoch),
                        "administration_action_cancelled"
                    );
                }
                Err(_) => anyhow::bail!("administration_action_cancelled"),
            }
        }
    }
    fn execute_direct(
        self,
        client: std::sync::Weak<ClientCore>,
    ) -> anyhow::Result<AdministrationCompletion> {
        let Self { identity, command } = self;
        let Some(core) = client.upgrade() else {
            anyhow::bail!("administration_action_cancelled");
        };
        anyhow::ensure!(
            core.administration_operation_current(&identity),
            "administration_action_cancelled"
        );
        let commands = match command {
            AdministrationCommand::SetMemberWorkspaces {
                principal_id,
                selected,
            } => core.plan_member_workspace_selection(principal_id, selected),
            command => Ok(vec![command]),
        };
        let sender = core.compatibility_runtime().ws_command_sender();
        drop(core);
        let result = (|| {
            let mut completion = AdministrationCompletion::MemberWorkspacesChanged;
            for command in commands? {
                let core = client
                    .upgrade()
                    .ok_or_else(|| anyhow::anyhow!("administration_action_cancelled"))?;
                anyhow::ensure!(
                    core.administration_operation_current(&identity),
                    "administration_action_cancelled"
                );
                anyhow::ensure!(
                    core.administration_command_allowed(&command),
                    "administration_action_forbidden"
                );
                drop(core);
                completion = match command {
                    AdministrationCommand::CreateInvitation(p) => sender
                        .invitation_create(p)
                        .map(AdministrationCompletion::InvitationCreated),
                    AdministrationCommand::RevokeInvitation(p) => sender
                        .invitation_revoke(p)
                        .map(AdministrationCompletion::InvitationRevoked),
                    AdministrationCommand::SuspendMember(p) => sender
                        .member_suspend(p)
                        .map(AdministrationCompletion::MemberChanged),
                    AdministrationCommand::RestoreMember(p) => sender
                        .member_restore(p)
                        .map(AdministrationCompletion::MemberChanged),
                    AdministrationCommand::RemoveMember(p) => sender
                        .member_remove(p)
                        .map(AdministrationCompletion::MemberChanged),
                    AdministrationCommand::CreateRecoveryDevice(p) => sender
                        .member_device_create(p)
                        .map(AdministrationCompletion::RecoveryDeviceCreated),
                    AdministrationCommand::AddWorkspaceMember(p) => sender
                        .workspace_member_add(p)
                        .map(AdministrationCompletion::WorkspaceMemberChanged),
                    AdministrationCommand::RemoveWorkspaceMember(p) => sender
                        .workspace_member_remove(p)
                        .map(AdministrationCompletion::WorkspaceMemberChanged),
                    AdministrationCommand::SetMemberWorkspaces { .. } => {
                        unreachable!("workspace selection is planned before transport")
                    }
                }?;
            }
            if matches!(
                identity.action,
                AdministrationAction::SetMemberWorkspaces { .. }
            ) {
                Ok(AdministrationCompletion::MemberWorkspacesChanged)
            } else {
                Ok(completion)
            }
        })();
        client
            .upgrade()
            .ok_or_else(|| anyhow::anyhow!("administration_action_cancelled"))?
            .finish_administration_command(&identity, result)
    }
}
