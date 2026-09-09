//! Client-owned administration workflows and secret-free projections.
//!
//! The Gateway remains the authorization and data authority. This module keeps
//! only snapshots returned by authenticated list methods and drops them when a
//! scoped notification says they may be stale.

pub mod pages;
pub mod operations;

pub mod types {
    pub use pioneer_protocol::{AuthMeResponse, AuthorizationCapabilitySnapshot, AuthorizationInvitationRoleOption,
        AuthSessionRevokeParams, AuthSessionStatus, MemberDeviceCreateParams, MemberListParams,
        MemberRemoveParams, MemberRestoreParams, MemberSummary, MemberSuspendParams, PrincipalId,
        PrincipalKind, PrincipalStatus, WorkspaceId, WorkspaceMemberAddParams, WorkspaceMemberListParams,
        WorkspaceMemberRemoveParams, InvitationCreateParams, InvitationId, InvitationListParams,
        InvitationRevokeParams, InvitationSummary, RoleKey, Workspace};
}

use pioneer_protocol::{
    InvitationChangedNotification, InvitationId,
    InvitationStatus, InvitationSummary, MemberChangedNotification,
    MemberSummary, PrincipalId, PrincipalKind, PrincipalStatus, RoleKey,
    WorkspaceId, WorkspaceMembersChangedNotification,
};
use std::collections::BTreeMap;

use crate::authorization::PrincipalPresentationCapabilities;

/// Shell-neutral invitation state. `Unknown` keeps newer server values
/// fail-closed in an older presentation layer.
#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InvitationPresentationStatus {
    Pending,
    Accepted,
    Revoked,
    Expired,
    #[default]
    Unknown,
}

impl InvitationPresentationStatus {
    pub fn from_protocol(status: Option<InvitationStatus>) -> Self {
        match status {
            Some(InvitationStatus::Pending) => Self::Pending,
            Some(InvitationStatus::Accepted) => Self::Accepted,
            Some(InvitationStatus::Revoked) => Self::Revoked,
            Some(InvitationStatus::Expired) => Self::Expired,
            None => Self::Unknown,
        }
    }

    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending)
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InvitationListRow {
    pub invitation_id: InvitationId,
    pub status: InvitationPresentationStatus,
    pub inviter_display_name: String,
    pub workspace_names: Vec<String>,
    pub created_at_unix: u64,
    pub expires_at_unix: u64,
    pub terminal_at_unix: Option<u64>,
    pub can_revoke: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MemberPresentationStatus {
    Active,
    Suspended,
    Removed,
    #[default]
    Unknown,
}

impl MemberPresentationStatus {
    pub fn from_protocol(status: Option<PrincipalStatus>) -> Self {
        match status {
            Some(PrincipalStatus::Active) => Self::Active,
            Some(PrincipalStatus::Suspended) => Self::Suspended,
            Some(PrincipalStatus::Removed) => Self::Removed,
            None => Self::Unknown,
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemberPresentationActions {
    pub can_suspend: bool,
    pub can_restore: bool,
    pub can_remove: bool,
    pub can_create_recovery_device: bool,
    pub can_add_to_workspace: bool,
    pub can_remove_from_workspace: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MemberListRow {
    pub principal_id: PrincipalId,
    pub kind: PrincipalKind,
    pub display_name: String,
    pub nickname: String,
    pub role_key: Option<RoleKey>,
    pub role: pioneer_protocol::AuthorizationRolePresentation,
    pub lifecycle_managed: bool,
    pub status: MemberPresentationStatus,
    /// Revision-addressed key for the authenticated HTTP avatar cache.
    pub avatar_revision: Option<String>,
    pub actions: MemberPresentationActions,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AdministrationAction {
    SetMemberWorkspaces { principal_id: PrincipalId },
    CreateInvitation,
    RevokeInvitation {
        invitation_id: InvitationId,
    },
    SuspendMember {
        principal_id: PrincipalId,
    },
    RestoreMember {
        principal_id: PrincipalId,
    },
    RemoveMember {
        principal_id: PrincipalId,
    },
    CreateRecoveryDevice {
        principal_id: PrincipalId,
    },
    AddWorkspaceMember {
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
    },
    RemoveWorkspaceMember {
        workspace_id: WorkspaceId,
        principal_id: PrincipalId,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum AdministrationPendingAction {
    #[default]
    Idle,
    Pending {
        action: AdministrationAction,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AdministrationRefetch {
    InvitationList,
    MemberDirectory,
    WorkspaceMembers { workspace_id: WorkspaceId },
}

pub fn invitation_list_row(
    invitation: &InvitationSummary,
    capabilities: PrincipalPresentationCapabilities,
) -> InvitationListRow {
    let status = InvitationPresentationStatus::from_protocol(Some(invitation.status));
    InvitationListRow {
        invitation_id: invitation.invitation_id.clone(),
        status,
        inviter_display_name: invitation.inviter.display_name.clone(),
        workspace_names: invitation
            .workspaces
            .iter()
            .map(|workspace| workspace.name.clone())
            .collect(),
        created_at_unix: invitation.created_at_unix,
        expires_at_unix: invitation.expires_at_unix,
        terminal_at_unix: invitation.terminal_at_unix,
        can_revoke: capabilities.can_create_invitation
            && status == InvitationPresentationStatus::Pending,
    }
}

pub fn member_list_row(
    member: &MemberSummary,
    current_principal_id: Option<&PrincipalId>,
    capabilities: PrincipalPresentationCapabilities,
    is_workspace_member: bool,
) -> MemberListRow {
    let status = MemberPresentationStatus::from_protocol(Some(member.status));
    let is_self = current_principal_id == Some(&member.principal_id);
    let manageable_target = member.lifecycle_managed && !is_self;
    let lifecycle = capabilities.can_manage_member_lifecycle && manageable_target;
    MemberListRow {
        principal_id: member.principal_id.clone(),
        kind: member.kind,
        display_name: member.display_name.clone(),
        nickname: member.nickname.clone(),
        role_key: member.role_key.clone(),
        role: member.role.clone(),
        lifecycle_managed: member.lifecycle_managed,
        status,
        avatar_revision: member.avatar_revision.clone(),
        actions: MemberPresentationActions {
            can_suspend: lifecycle && status == MemberPresentationStatus::Active,
            can_restore: lifecycle && status == MemberPresentationStatus::Suspended,
            can_remove: lifecycle && status != MemberPresentationStatus::Removed,
            can_create_recovery_device: lifecycle && status == MemberPresentationStatus::Active,
            can_add_to_workspace: capabilities.can_add_workspace_member
                && manageable_target
                && status == MemberPresentationStatus::Active
                && !is_workspace_member,
            can_remove_from_workspace: capabilities.can_remove_workspace_member
                && manageable_target
                && status == MemberPresentationStatus::Active
                && is_workspace_member,
        },
    }
}

/// A conflict means the cached precondition lost a race. The shell must clear
/// the spinner and refetch the smallest authoritative snapshot instead of
/// guessing the new state.
pub fn conflict_refetch(action: &AdministrationAction) -> Vec<AdministrationRefetch> {
    match action {
        AdministrationAction::CreateInvitation | AdministrationAction::RevokeInvitation { .. } => {
            vec![AdministrationRefetch::InvitationList]
        }
        AdministrationAction::SetMemberWorkspaces { .. }
        | AdministrationAction::SuspendMember { .. }
        | AdministrationAction::RestoreMember { .. }
        | AdministrationAction::RemoveMember { .. }
        | AdministrationAction::CreateRecoveryDevice { .. } => {
            vec![AdministrationRefetch::MemberDirectory]
        }
        AdministrationAction::AddWorkspaceMember { workspace_id, .. }
        | AdministrationAction::RemoveWorkspaceMember { workspace_id, .. } => vec![
            AdministrationRefetch::MemberDirectory,
            AdministrationRefetch::WorkspaceMembers {
                workspace_id: workspace_id.clone(),
            },
        ],
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdministrationEvent {
    InvitationChanged(InvitationChangedNotification),
    MemberChanged(MemberChangedNotification),
    WorkspaceMembersChanged(WorkspaceMembersChangedNotification),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdministrationInvalidation {
    pub apply: bool,
    pub effects: Vec<AdministrationRefetch>,
}

/// Deduplicates realtime hints before the Client page owner refreshes its
/// authoritative rows. It does not retain a second snapshot cache.
#[derive(Default)]
pub(crate) struct AdministrationEventTracker {
    invitation_revisions: BTreeMap<InvitationId, u64>,
    member_revisions: BTreeMap<PrincipalId, u64>,
    workspace_member_revisions: BTreeMap<WorkspaceId, u64>,
}

impl AdministrationEventTracker {
    pub fn apply_event(&mut self, event: &AdministrationEvent) -> AdministrationInvalidation {
        match event {
            AdministrationEvent::InvitationChanged(notification) => {
                if is_stale(
                    self.invitation_revisions.get(&notification.invitation_id),
                    notification.revision,
                ) {
                    return no_change();
                }
                self.invitation_revisions
                    .insert(notification.invitation_id.clone(), notification.revision);
                changed(AdministrationRefetch::InvitationList)
            }
            AdministrationEvent::MemberChanged(notification) => {
                if is_stale(
                    self.member_revisions.get(&notification.principal_id),
                    notification.revision,
                ) {
                    return no_change();
                }
                self.member_revisions
                    .insert(notification.principal_id.clone(), notification.revision);
                changed(AdministrationRefetch::MemberDirectory)
            }
            AdministrationEvent::WorkspaceMembersChanged(notification) => {
                if is_stale(
                    self.workspace_member_revisions
                        .get(&notification.workspace_id),
                    notification.revision,
                ) {
                    return no_change();
                }
                self.workspace_member_revisions
                    .insert(notification.workspace_id.clone(), notification.revision);
                AdministrationInvalidation {
                    apply: true,
                    effects: vec![
                        AdministrationRefetch::MemberDirectory,
                        AdministrationRefetch::WorkspaceMembers {
                            workspace_id: notification.workspace_id.clone(),
                        },
                    ],
                }
            }
        }
    }


}

fn is_stale(previous: Option<&u64>, revision: u64) -> bool { previous.is_some_and(|previous| *previous >= revision) }

fn no_change() -> AdministrationInvalidation {
    AdministrationInvalidation {
        apply: false,
        effects: Vec::new(),
    }
}

fn changed(effect: AdministrationRefetch) -> AdministrationInvalidation {
    AdministrationInvalidation {
        apply: true,
        effects: vec![effect],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        MemberSummary, RoleKey,
    };

    fn member(principal_id: &str) -> MemberSummary {
        MemberSummary {
            principal_id: PrincipalId::new(principal_id).expect("valid principal id"),
            kind: PrincipalKind::User,
            display_name: principal_id.to_owned(),
            nickname: principal_id.to_owned(),
            role_key: Some(RoleKey::member()),
            role: pioneer_protocol::AuthorizationRolePresentation {
                key: "member".to_owned(),
                display_name: "Member".to_owned(),
                description: "Workspace collaborator".to_owned(),
                built_in: true,
            },
            lifecycle_managed: true,
            status: PrincipalStatus::Active,
            avatar_revision: None,
        }
    }

    #[test]
    fn presentation_fails_closed_for_unknown_states_and_scopes_actions() {
        assert_eq!(
            InvitationPresentationStatus::from_protocol(None),
            InvitationPresentationStatus::Unknown
        );
        assert!(InvitationPresentationStatus::Unknown.is_terminal());
        assert_eq!(
            MemberPresentationStatus::from_protocol(None),
            MemberPresentationStatus::Unknown
        );

        let target = member("PAAAAAAAAAAAAAAAAAAAA");
        let root = PrincipalPresentationCapabilities {
            can_create_workspace: true,
            can_manage_workspace: true,
            can_read_own_notifications: true,
            can_acknowledge_own_notifications: true,
            can_manage_gateway_settings: true,
            can_manage_capabilities: true,
            can_use_providers: true,
            can_use_cli_runtimes: true,
            can_use_skills: true,
            can_use_mcp: true,
            can_run_tasks: true,
            can_manage_all_threads: true,
            can_view_invitations: true,
            can_create_invitation: true,
            can_view_member_directory: true,
            can_add_workspace_member: true,
            can_manage_member_lifecycle: true,
            can_remove_workspace_member: true,
            can_manage_own_sessions: true,
        };
        let row = member_list_row(&target, None, root, true);
        assert!(row.actions.can_suspend);
        assert!(row.actions.can_remove_from_workspace);
        assert!(!row.actions.can_restore);
        assert!(!row.actions.can_add_to_workspace);

        let member_capabilities = PrincipalPresentationCapabilities {
            can_create_workspace: false,
            can_manage_workspace: false,
            can_read_own_notifications: true,
            can_acknowledge_own_notifications: true,
            can_manage_gateway_settings: false,
            can_manage_capabilities: false,
            can_use_providers: true,
            can_use_cli_runtimes: true,
            can_use_skills: true,
            can_use_mcp: true,
            can_run_tasks: true,
            can_manage_all_threads: false,
            can_view_invitations: true,
            can_create_invitation: true,
            can_view_member_directory: true,
            can_add_workspace_member: true,
            can_manage_member_lifecycle: false,
            can_remove_workspace_member: false,
            can_manage_own_sessions: true,
        };
        let addable = member_list_row(&target, None, member_capabilities, false);
        assert!(addable.actions.can_add_to_workspace);
        assert!(!addable.actions.can_remove_from_workspace);
        assert!(!addable.actions.can_suspend);

        let self_row = member_list_row(&target, Some(&target.principal_id), root, true);
        assert_eq!(
            self_row.actions,
            MemberPresentationActions {
                can_suspend: false,
                can_restore: false,
                can_remove: false,
                can_create_recovery_device: false,
                can_add_to_workspace: false,
                can_remove_from_workspace: false,
            }
        );

        let mut externally_managed_role = target.clone();
        externally_managed_role.role_key = Some(RoleKey::new("external_operator").unwrap());
        externally_managed_role.role = pioneer_protocol::AuthorizationRolePresentation {
            key: "external_operator".to_owned(),
            display_name: "External operator".to_owned(),
            description: "Managed by an external identity policy".to_owned(),
            built_in: false,
        };
        externally_managed_role.lifecycle_managed = false;
        let external_row = member_list_row(&externally_managed_role, None, root, true);
        assert_eq!(external_row.actions, self_row.actions);

        let unknown = member_list_row(
            &target,
            None,
            PrincipalPresentationCapabilities::default(),
            false,
        );
        assert_eq!(unknown.actions, self_row.actions);
    }


}

mod reads;
