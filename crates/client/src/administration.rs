//! Secret-free, non-authoritative client projections for Epic 5 administration.
//!
//! The Gateway remains the authorization and data authority. This module keeps
//! only snapshots returned by authenticated list methods and drops them when a
//! scoped notification says they may be stale.

use pioneer_protocol::{
    AccessChangeKind, AccessChangedNotification, InvitationChangedNotification, InvitationId,
    InvitationListResponse, InvitationSummary, MemberChangedNotification, MemberListResponse,
    MemberSummary, PrincipalId, WorkspaceId, WorkspaceMemberListResponse,
    WorkspaceMembersChangedNotification,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdministrationRefetch {
    InvitationList,
    MemberDirectory,
    WorkspaceMembers { workspace_id: WorkspaceId },
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

#[derive(Default)]
pub struct AdministrationCache {
    invitations: Vec<InvitationSummary>,
    members: Vec<MemberSummary>,
    workspace_members: BTreeMap<WorkspaceId, Vec<MemberSummary>>,
    invitation_revisions: BTreeMap<InvitationId, u64>,
    member_revisions: BTreeMap<PrincipalId, u64>,
    workspace_member_revisions: BTreeMap<WorkspaceId, u64>,
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

    pub fn apply_invitation_list(&mut self, response: InvitationListResponse) {
        self.invitations = response.invitations;
    }

    /// Appends a subsequent cursor page while preserving Gateway order.
    ///
    /// The ordinary `apply_*` methods replace an authoritative snapshot so a
    /// reconnect/refetch can remove stale rows. Pagination is explicit and
    /// deduplicates an overlapping boundary without changing server order.
    pub fn append_invitation_page(&mut self, response: InvitationListResponse) {
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
    }

    pub fn append_member_page(&mut self, response: MemberListResponse) {
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
        self.workspace_members
            .insert(response.workspace_id, response.members);
    }

    pub fn append_workspace_member_page(&mut self, response: WorkspaceMemberListResponse) {
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
            self.member_revisions.clear();
            return AdministrationInvalidation {
                apply: true,
                effects: vec![AdministrationRefetch::MemberDirectory],
            };
        };
        self.workspace_members.remove(&workspace_id);
        self.workspace_member_revisions.remove(&workspace_id);

        // The directory visibility predicate depends on shared workspace
        // membership, so no individual cached member can be proven visible
        // after this change. Other workspace snapshots remain intact.
        self.members.clear();
        self.member_revisions.clear();

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
        if is_stale(
            self.invitation_revisions.get(&notification.invitation_id),
            notification.revision,
        ) {
            return no_change();
        }
        self.invitation_revisions
            .insert(notification.invitation_id.clone(), notification.revision);
        self.invitations
            .retain(|invitation| invitation.invitation_id != notification.invitation_id);
        changed(AdministrationRefetch::InvitationList)
    }

    fn invalidate_member(
        &mut self,
        notification: &MemberChangedNotification,
    ) -> AdministrationInvalidation {
        if is_stale(
            self.member_revisions.get(&notification.principal_id),
            notification.revision,
        ) {
            return no_change();
        }
        self.member_revisions
            .insert(notification.principal_id.clone(), notification.revision);
        self.members
            .retain(|member| member.principal_id != notification.principal_id);
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
        if is_stale(
            self.workspace_member_revisions
                .get(&notification.workspace_id),
            notification.revision,
        ) {
            return no_change();
        }
        self.workspace_member_revisions
            .insert(notification.workspace_id.clone(), notification.revision);
        self.workspace_members.remove(&notification.workspace_id);
        // Directory visibility for ordinary Members is the union of current
        // shared workspace memberships. Any membership change can therefore
        // add or remove directory rows even when no profile itself changed.
        self.members.clear();
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
        PrincipalKind, PrincipalStatus, RoleKey,
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
    }
}
