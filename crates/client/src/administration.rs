//! Secret-free, non-authoritative client projections for Epic 5 administration.
//!
//! The Gateway remains the authorization and data authority. This module keeps
//! only snapshots returned by authenticated list methods and drops them when a
//! scoped notification says they may be stale.

use pioneer_protocol::{
    AccessChangeKind, AccessChangedNotification, InvitationChangedNotification, InvitationId,
    InvitationListResponse, InvitationStatus, InvitationSummary, MemberChangedNotification,
    MemberListResponse, MemberSummary, PrincipalId, PrincipalKind, PrincipalStatus, RoleKey,
    WorkspaceId, WorkspaceMemberListResponse, WorkspaceMembersChangedNotification,
};
use std::collections::{BTreeMap, BTreeSet};

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
    pub status: MemberPresentationStatus,
    /// Revision-addressed key for the authenticated HTTP avatar cache.
    pub avatar_revision: Option<String>,
    pub actions: MemberPresentationActions,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AdministrationAction {
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
    let manageable_target = member.kind == PrincipalKind::User && !is_self;
    let lifecycle = capabilities.can_manage_member_lifecycle && manageable_target;
    MemberListRow {
        principal_id: member.principal_id.clone(),
        kind: member.kind,
        display_name: member.display_name.clone(),
        nickname: member.nickname.clone(),
        role_key: member.role_key.clone(),
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
        AdministrationAction::SuspendMember { .. }
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

/// Revision-only reducer for shells whose authoritative administration rows
/// live elsewhere (for example TanStack Query). It rejects stale realtime
/// hints without becoming a second snapshot cache.
#[derive(Default)]
pub struct AdministrationEventTracker {
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

    fn clear_member_revisions(&mut self) {
        self.member_revisions.clear();
    }

    fn remove_workspace(&mut self, workspace_id: &WorkspaceId) {
        self.workspace_member_revisions.remove(workspace_id);
    }
}

#[derive(Default)]
pub struct AdministrationCache {
    invitations: Vec<InvitationSummary>,
    members: Vec<MemberSummary>,
    workspace_members: BTreeMap<WorkspaceId, Vec<MemberSummary>>,
    invitation_next_cursor: Option<String>,
    member_next_cursor: Option<String>,
    workspace_member_next_cursors: BTreeMap<WorkspaceId, Option<String>>,
    pending_action: AdministrationPendingAction,
    event_tracker: AdministrationEventTracker,
}

impl AdministrationCache {
    pub fn invitations(&self) -> impl Iterator<Item = &InvitationSummary> {
        self.invitations.iter()
    }

    pub fn members(&self) -> impl Iterator<Item = &MemberSummary> {
        self.members.iter()
    }

    pub fn workspace_members(&self, workspace_id: &WorkspaceId) -> Option<&[MemberSummary]> {
        self.workspace_members.get(workspace_id).map(Vec::as_slice)
    }

    pub fn invitation_next_cursor(&self) -> Option<&str> {
        self.invitation_next_cursor.as_deref()
    }

    pub fn member_next_cursor(&self) -> Option<&str> {
        self.member_next_cursor.as_deref()
    }

    pub fn workspace_member_next_cursor(&self, workspace_id: &WorkspaceId) -> Option<&str> {
        self.workspace_member_next_cursors
            .get(workspace_id)
            .and_then(Option::as_deref)
    }

    pub fn pending_action(&self) -> &AdministrationPendingAction {
        &self.pending_action
    }

    pub fn begin_action(&mut self, action: AdministrationAction) -> bool {
        if !matches!(self.pending_action, AdministrationPendingAction::Idle) {
            return false;
        }
        self.pending_action = AdministrationPendingAction::Pending { action };
        true
    }

    pub fn finish_action(&mut self) {
        self.pending_action = AdministrationPendingAction::Idle;
    }

    pub fn finish_conflicted_action(&mut self) -> Vec<AdministrationRefetch> {
        let AdministrationPendingAction::Pending { action } = &self.pending_action else {
            return Vec::new();
        };
        let effects = conflict_refetch(action);
        self.pending_action = AdministrationPendingAction::Idle;
        effects
    }

    pub fn apply_invitation_list(&mut self, response: InvitationListResponse) {
        self.invitations = response.invitations;
        self.invitation_next_cursor = response.next_cursor;
    }

    /// Appends a subsequent cursor page while preserving Gateway order.
    ///
    /// The ordinary `apply_*` methods replace an authoritative snapshot so a
    /// reconnect/refetch can remove stale rows. Pagination is explicit and
    /// deduplicates an overlapping boundary without changing server order.
    pub fn append_invitation_page(&mut self, response: InvitationListResponse) {
        self.invitation_next_cursor = response.next_cursor.clone();
        let mut known = self
            .invitations
            .iter()
            .map(|invitation| invitation.invitation_id.clone())
            .collect::<BTreeSet<_>>();
        self.invitations.extend(
            response
                .invitations
                .into_iter()
                .filter(|invitation| known.insert(invitation.invitation_id.clone())),
        );
    }

    pub fn apply_member_list(&mut self, response: MemberListResponse) {
        self.members = response.members;
        self.member_next_cursor = response.next_cursor;
    }

    pub fn append_member_page(&mut self, response: MemberListResponse) {
        self.member_next_cursor = response.next_cursor.clone();
        let mut known = self
            .members
            .iter()
            .map(|member| member.principal_id.clone())
            .collect::<BTreeSet<_>>();
        self.members.extend(
            response
                .members
                .into_iter()
                .filter(|member| known.insert(member.principal_id.clone())),
        );
    }

    pub fn apply_workspace_member_list(&mut self, response: WorkspaceMemberListResponse) {
        self.workspace_member_next_cursors
            .insert(response.workspace_id.clone(), response.next_cursor.clone());
        self.workspace_members
            .insert(response.workspace_id, response.members);
    }

    pub fn append_workspace_member_page(&mut self, response: WorkspaceMemberListResponse) {
        self.workspace_member_next_cursors
            .insert(response.workspace_id.clone(), response.next_cursor.clone());
        let members = self
            .workspace_members
            .entry(response.workspace_id)
            .or_default();
        let mut known = members
            .iter()
            .map(|member| member.principal_id.clone())
            .collect::<BTreeSet<_>>();
        members.extend(
            response
                .members
                .into_iter()
                .filter(|member| known.insert(member.principal_id.clone())),
        );
    }

    pub fn apply_event(&mut self, event: &AdministrationEvent) -> AdministrationInvalidation {
        let tracked = self.event_tracker.apply_event(event);
        if !tracked.apply {
            return tracked;
        }
        match event {
            AdministrationEvent::InvitationChanged(notification) => {
                self.invalidate_invitation(notification)
            }
            AdministrationEvent::MemberChanged(notification) => {
                self.invalidate_member(notification)
            }
            AdministrationEvent::WorkspaceMembersChanged(notification) => {
                self.invalidate_workspace_members(notification)
            }
        }
    }

    pub fn apply_access_changed(
        &mut self,
        notification: &AccessChangedNotification,
    ) -> AdministrationInvalidation {
        if notification.change != AccessChangeKind::WorkspaceMembership {
            return AdministrationInvalidation {
                apply: false,
                effects: Vec::new(),
            };
        }

        let Ok(workspace_id) = WorkspaceId::new(notification.workspace_id.clone()) else {
            self.members.clear();
            self.member_next_cursor = None;
            self.event_tracker.clear_member_revisions();
            return AdministrationInvalidation {
                apply: true,
                effects: vec![AdministrationRefetch::MemberDirectory],
            };
        };
        self.workspace_members.remove(&workspace_id);
        self.workspace_member_next_cursors.remove(&workspace_id);
        self.event_tracker.remove_workspace(&workspace_id);

        // The directory visibility predicate depends on shared workspace
        // membership, so no individual cached member can be proven visible
        // after this change. Other workspace snapshots remain intact.
        self.members.clear();
        self.member_next_cursor = None;
        self.event_tracker.clear_member_revisions();

        AdministrationInvalidation {
            apply: true,
            effects: vec![
                AdministrationRefetch::MemberDirectory,
                AdministrationRefetch::WorkspaceMembers { workspace_id },
            ],
        }
    }

    pub fn clear_for_session_termination(&mut self) {
        *self = Self::default();
    }

    fn invalidate_invitation(
        &mut self,
        notification: &InvitationChangedNotification,
    ) -> AdministrationInvalidation {
        self.invitations
            .retain(|invitation| invitation.invitation_id != notification.invitation_id);
        self.invitation_next_cursor = None;
        changed(AdministrationRefetch::InvitationList)
    }

    fn invalidate_member(
        &mut self,
        notification: &MemberChangedNotification,
    ) -> AdministrationInvalidation {
        self.members
            .retain(|member| member.principal_id != notification.principal_id);
        self.member_next_cursor = None;
        let affected_workspaces = self
            .workspace_members
            .iter()
            .filter(|(_, members)| {
                members
                    .iter()
                    .any(|member| member.principal_id == notification.principal_id)
            })
            .map(|(workspace_id, _)| workspace_id.clone())
            .collect::<Vec<_>>();
        for workspace_id in &affected_workspaces {
            self.workspace_members.remove(workspace_id);
            self.workspace_member_next_cursors.remove(workspace_id);
        }
        let mut effects = vec![AdministrationRefetch::MemberDirectory];
        effects.extend(
            affected_workspaces
                .into_iter()
                .map(|workspace_id| AdministrationRefetch::WorkspaceMembers { workspace_id }),
        );
        AdministrationInvalidation {
            apply: true,
            effects,
        }
    }

    fn invalidate_workspace_members(
        &mut self,
        notification: &WorkspaceMembersChangedNotification,
    ) -> AdministrationInvalidation {
        self.workspace_members.remove(&notification.workspace_id);
        self.workspace_member_next_cursors
            .remove(&notification.workspace_id);
        // Directory visibility for ordinary Members is the union of current
        // shared workspace memberships. Any membership change can therefore
        // add or remove directory rows even when no profile itself changed.
        self.members.clear();
        self.member_next_cursor = None;
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

fn is_stale(previous: Option<&u64>, revision: u64) -> bool {
    previous.is_some_and(|previous| *previous >= revision)
}

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
        InvitationInviterSummary, InvitationStatus, InvitationWorkspaceSummary, MemberSummary,
        RoleKey,
    };

    fn member(principal_id: &str) -> MemberSummary {
        MemberSummary {
            principal_id: PrincipalId::new(principal_id).expect("valid principal id"),
            kind: PrincipalKind::User,
            display_name: principal_id.to_owned(),
            nickname: principal_id.to_owned(),
            role_key: Some(RoleKey::member()),
            status: PrincipalStatus::Active,
            avatar_revision: None,
        }
    }

    fn invitation(invitation_id: &str) -> InvitationSummary {
        InvitationSummary {
            invitation_id: InvitationId::new(invitation_id).expect("valid invitation id"),
            status: InvitationStatus::Pending,
            revoke_reason: None,
            inviter: InvitationInviterSummary {
                principal_id: PrincipalId::new("PIIIIIIIIIIIIIIIIIIII")
                    .expect("valid principal id"),
                kind: PrincipalKind::Superuser,
                display_name: "Inviter".to_owned(),
                nickname: "inviter".to_owned(),
            },
            workspaces: vec![InvitationWorkspaceSummary {
                workspace_id: WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA")
                    .expect("valid workspace id"),
                name: "A".to_owned(),
            }],
            created_at_unix: 1,
            expires_at_unix: 2,
            terminal_at_unix: None,
        }
    }

    #[test]
    fn scoped_events_drop_only_the_affected_snapshot_and_reject_stale_revisions() {
        let mut cache = AdministrationCache::default();
        cache.apply_invitation_list(InvitationListResponse {
            invitations: vec![
                invitation("IAAAAAAAAAAAAAAAAAAAA"),
                invitation("IBBBBBBBBBBBBBBBBBBBB"),
            ],
            next_cursor: None,
        });

        let changed = AdministrationEvent::InvitationChanged(InvitationChangedNotification {
            revision: 7,
            invitation_id: InvitationId::new("IAAAAAAAAAAAAAAAAAAAA").expect("valid invitation id"),
        });
        let plan = cache.apply_event(&changed);
        assert_eq!(plan.effects, vec![AdministrationRefetch::InvitationList]);
        assert_eq!(cache.invitations().count(), 1);
        assert_eq!(
            cache.invitations().next().unwrap().invitation_id,
            InvitationId::new("IBBBBBBBBBBBBBBBBBBBB").expect("valid invitation id")
        );

        let stale = cache.apply_event(&changed);
        assert!(!stale.apply);
    }

    #[test]
    fn workspace_access_change_preserves_unrelated_workspace_snapshot() {
        let mut cache = AdministrationCache::default();
        cache.apply_member_list(MemberListResponse {
            members: vec![member("PAAAAAAAAAAAAAAAAAAAA")],
            next_cursor: None,
        });
        cache.apply_workspace_member_list(WorkspaceMemberListResponse {
            workspace_id: WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA").expect("valid workspace id"),
            members: vec![member("PAAAAAAAAAAAAAAAAAAAA")],
            next_cursor: None,
        });
        cache.apply_workspace_member_list(WorkspaceMemberListResponse {
            workspace_id: WorkspaceId::new("WBBBBBBBBBBBBBBBBBBBB").expect("valid workspace id"),
            members: vec![member("PBBBBBBBBBBBBBBBBBBBB")],
            next_cursor: None,
        });

        let plan = cache.apply_access_changed(&AccessChangedNotification {
            authorization_revision: 9,
            workspace_id: "WAAAAAAAAAAAAAAAAAAAA".to_owned(),
            thread_id: None,
            change: AccessChangeKind::WorkspaceMembership,
        });

        assert!(plan.apply);
        assert!(cache.members().next().is_none());
        assert!(
            cache
                .workspace_members(
                    &WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA").expect("valid workspace id"),
                )
                .is_none()
        );
        assert_eq!(
            cache
                .workspace_members(
                    &WorkspaceId::new("WBBBBBBBBBBBBBBBBBBBB").expect("valid workspace id"),
                )
                .unwrap()[0]
                .principal_id,
            PrincipalId::new("PBBBBBBBBBBBBBBBBBBBB").expect("valid principal id")
        );
    }

    #[test]
    fn administration_events_invalidate_every_snapshot_derived_from_the_changed_membership() {
        let workspace_id = WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA").expect("valid workspace id");
        let principal_id = PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").expect("valid principal id");
        let mut cache = AdministrationCache::default();
        cache.apply_member_list(MemberListResponse {
            members: vec![member(principal_id.as_str())],
            next_cursor: None,
        });
        cache.apply_workspace_member_list(WorkspaceMemberListResponse {
            workspace_id: workspace_id.clone(),
            members: vec![member(principal_id.as_str())],
            next_cursor: None,
        });

        let member_plan = cache.apply_event(&AdministrationEvent::MemberChanged(
            MemberChangedNotification {
                revision: 10,
                principal_id: principal_id.clone(),
            },
        ));
        assert_eq!(
            member_plan.effects,
            vec![
                AdministrationRefetch::MemberDirectory,
                AdministrationRefetch::WorkspaceMembers {
                    workspace_id: workspace_id.clone(),
                },
            ]
        );
        assert!(cache.members().next().is_none());
        assert!(cache.workspace_members(&workspace_id).is_none());

        cache.apply_member_list(MemberListResponse {
            members: vec![member(principal_id.as_str())],
            next_cursor: None,
        });
        cache.apply_workspace_member_list(WorkspaceMemberListResponse {
            workspace_id: workspace_id.clone(),
            members: vec![member(principal_id.as_str())],
            next_cursor: None,
        });
        let membership_plan = cache.apply_event(&AdministrationEvent::WorkspaceMembersChanged(
            WorkspaceMembersChangedNotification {
                revision: 11,
                workspace_id: workspace_id.clone(),
            },
        ));
        assert_eq!(
            membership_plan.effects,
            vec![
                AdministrationRefetch::MemberDirectory,
                AdministrationRefetch::WorkspaceMembers {
                    workspace_id: workspace_id.clone(),
                },
            ]
        );
        assert!(cache.members().next().is_none());
        assert!(cache.workspace_members(&workspace_id).is_none());
    }

    #[test]
    fn session_termination_clears_cache() {
        let mut cache = AdministrationCache::default();
        cache.apply_member_list(MemberListResponse {
            members: vec![member("PAAAAAAAAAAAAAAAAAAAA")],
            next_cursor: None,
        });
        assert_eq!(cache.members().count(), 1);

        cache.clear_for_session_termination();
        assert_eq!(cache.members().count(), 0);
    }

    #[test]
    fn authoritative_refetch_replaces_stale_rows_and_pages_append_in_server_order() {
        let mut cache = AdministrationCache::default();
        cache.apply_invitation_list(InvitationListResponse {
            invitations: vec![
                invitation("IBBBBBBBBBBBBBBBBBBBB"),
                invitation("IAAAAAAAAAAAAAAAAAAAA"),
            ],
            next_cursor: Some("page-2".to_owned()),
        });
        cache.append_invitation_page(InvitationListResponse {
            invitations: vec![
                invitation("IAAAAAAAAAAAAAAAAAAAA"),
                invitation("ICCCCCCCCCCCCCCCCCCCC"),
            ],
            next_cursor: None,
        });
        assert_eq!(
            cache
                .invitations()
                .map(|row| row.invitation_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "IBBBBBBBBBBBBBBBBBBBB",
                "IAAAAAAAAAAAAAAAAAAAA",
                "ICCCCCCCCCCCCCCCCCCCC",
            ]
        );

        cache.apply_invitation_list(InvitationListResponse {
            invitations: vec![invitation("ICCCCCCCCCCCCCCCCCCCC")],
            next_cursor: None,
        });
        assert_eq!(
            cache
                .invitations()
                .map(|row| row.invitation_id.as_str())
                .collect::<Vec<_>>(),
            vec!["ICCCCCCCCCCCCCCCCCCCC"]
        );

        cache.apply_member_list(MemberListResponse {
            members: vec![
                member("PBBBBBBBBBBBBBBBBBBBB"),
                member("PAAAAAAAAAAAAAAAAAAAA"),
            ],
            next_cursor: Some("page-2".to_owned()),
        });
        cache.append_member_page(MemberListResponse {
            members: vec![
                member("PAAAAAAAAAAAAAAAAAAAA"),
                member("PCCCCCCCCCCCCCCCCCCCC"),
            ],
            next_cursor: None,
        });
        assert_eq!(
            cache
                .members()
                .map(|row| row.principal_id.as_str())
                .collect::<Vec<_>>(),
            vec![
                "PBBBBBBBBBBBBBBBBBBBB",
                "PAAAAAAAAAAAAAAAAAAAA",
                "PCCCCCCCCCCCCCCCCCCCC",
            ]
        );

        cache.apply_member_list(MemberListResponse {
            members: vec![member("PCCCCCCCCCCCCCCCCCCCC")],
            next_cursor: None,
        });
        assert_eq!(
            cache
                .members()
                .map(|row| row.principal_id.as_str())
                .collect::<Vec<_>>(),
            vec!["PCCCCCCCCCCCCCCCCCCCC"]
        );
    }

    #[test]
    fn workspace_member_pages_append_without_replacing_the_first_page() {
        let workspace_id = WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA").expect("valid workspace id");
        let mut cache = AdministrationCache::default();
        cache.apply_workspace_member_list(WorkspaceMemberListResponse {
            workspace_id: workspace_id.clone(),
            members: vec![member("PBBBBBBBBBBBBBBBBBBBB")],
            next_cursor: Some("page-2".to_owned()),
        });
        cache.append_workspace_member_page(WorkspaceMemberListResponse {
            workspace_id: workspace_id.clone(),
            members: vec![
                member("PBBBBBBBBBBBBBBBBBBBB"),
                member("PAAAAAAAAAAAAAAAAAAAA"),
            ],
            next_cursor: None,
        });

        assert_eq!(
            cache
                .workspace_members(&workspace_id)
                .expect("workspace snapshot")
                .iter()
                .map(|row| row.principal_id.as_str())
                .collect::<Vec<_>>(),
            vec!["PBBBBBBBBBBBBBBBBBBBB", "PAAAAAAAAAAAAAAAAAAAA"]
        );
        assert_eq!(cache.workspace_member_next_cursor(&workspace_id), None);
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

        let unknown = member_list_row(
            &target,
            None,
            PrincipalPresentationCapabilities::default(),
            false,
        );
        assert_eq!(unknown.actions, self_row.actions);
    }

    #[test]
    fn pagination_cursors_follow_the_authoritative_pages() {
        let mut cache = AdministrationCache::default();
        cache.apply_invitation_list(InvitationListResponse {
            invitations: vec![invitation("IAAAAAAAAAAAAAAAAAAAA")],
            next_cursor: Some("invite-2".to_owned()),
        });
        cache.apply_member_list(MemberListResponse {
            members: vec![member("PAAAAAAAAAAAAAAAAAAAA")],
            next_cursor: Some("member-2".to_owned()),
        });
        assert_eq!(cache.invitation_next_cursor(), Some("invite-2"));
        assert_eq!(cache.member_next_cursor(), Some("member-2"));

        cache.append_invitation_page(InvitationListResponse {
            invitations: vec![invitation("IBBBBBBBBBBBBBBBBBBBB")],
            next_cursor: None,
        });
        cache.append_member_page(MemberListResponse {
            members: vec![member("PBBBBBBBBBBBBBBBBBBBB")],
            next_cursor: None,
        });
        assert_eq!(cache.invitation_next_cursor(), None);
        assert_eq!(cache.member_next_cursor(), None);
    }

    #[test]
    fn pending_action_is_single_owner_and_conflict_refetch_is_scoped() {
        let workspace_id = WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA").expect("workspace id");
        let principal_id = PrincipalId::new("PAAAAAAAAAAAAAAAAAAAA").expect("principal id");
        let action = AdministrationAction::RemoveWorkspaceMember {
            workspace_id: workspace_id.clone(),
            principal_id,
        };
        let mut cache = AdministrationCache::default();
        assert!(cache.begin_action(action));
        assert!(!cache.begin_action(AdministrationAction::RevokeInvitation {
            invitation_id: InvitationId::new("IAAAAAAAAAAAAAAAAAAAA").expect("invitation id"),
        }));
        assert_eq!(
            cache.finish_conflicted_action(),
            vec![
                AdministrationRefetch::MemberDirectory,
                AdministrationRefetch::WorkspaceMembers { workspace_id },
            ]
        );
        assert_eq!(cache.pending_action(), &AdministrationPendingAction::Idle);
    }
}
