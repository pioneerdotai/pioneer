//! Shell-neutral presentation and mutation planning for user-thread scope.
//!
//! The Gateway remains the authority. These helpers only project already
//! authorized snapshots and deliberately fail closed for incomplete or future
//! values.

use std::collections::BTreeSet;

use pioneer_protocol::{
    AuthorizationWorkspaceCapabilities, MemberSummary, PrincipalId, PrincipalStatus, Thread,
    ThreadOriginKind, ThreadParticipantSummary, ThreadStatus, ThreadVisibility, WorkspaceId,
};

use crate::authorization::ThreadPresentationCapabilities;

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ThreadVisibilityPresentation {
    Private,
    Workspace,
    Unknown,
}

impl From<Option<ThreadVisibility>> for ThreadVisibilityPresentation {
    fn from(value: Option<ThreadVisibility>) -> Self {
        match value {
            Some(ThreadVisibility::Private) => Self::Private,
            Some(ThreadVisibility::Workspace) => Self::Workspace,
            None => Self::Unknown,
        }
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadCreateVisibilityPlan {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_visibility: Option<ThreadVisibility>,
    pub options: Vec<ThreadVisibility>,
}

/// User threads default to private. Recognized workspace Members and the
/// Superuser may choose either user-addressable visibility; internal/system
/// threads expose no selector.
pub fn thread_create_visibility_plan(
    capabilities: Option<&AuthorizationWorkspaceCapabilities>,
    origin_kind: ThreadOriginKind,
) -> ThreadCreateVisibilityPlan {
    if !is_user_thread_origin(origin_kind) {
        return ThreadCreateVisibilityPlan::default();
    }
    let Some(capabilities) = capabilities.filter(|value| value.can_create_thread) else {
        return ThreadCreateVisibilityPlan::default();
    };
    let options = capabilities.thread_visibility_options.clone();
    let default_visibility = options
        .iter()
        .copied()
        .find(|visibility| *visibility == ThreadVisibility::Private)
        .or_else(|| options.first().copied());
    ThreadCreateVisibilityPlan {
        default_visibility,
        options,
    }
}

const fn is_user_thread_origin(origin_kind: ThreadOriginKind) -> bool {
    matches!(
        origin_kind,
        ThreadOriginKind::Collaborative | ThreadOriginKind::DirectMessage | ThreadOriginKind::User
    )
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadParticipantRow {
    pub principal_id: PrincipalId,
    pub display_name: String,
    pub nickname: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub avatar_revision: Option<String>,
    pub is_current_principal: bool,
    pub can_remove: bool,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadScopePresentation {
    pub visibility: ThreadVisibilityPresentation,
    pub is_user_thread: bool,
    pub is_closed: bool,
    pub capabilities: ThreadPresentationCapabilities,
    pub participants: Vec<ThreadParticipantRow>,
    pub candidate_members: Vec<ThreadParticipantRow>,
    pub show_workspace_explanation: bool,
}

/// Project only principals disclosed by the current workspace Member snapshot.
/// Unknown participant identifiers are omitted rather than becoming a probing
/// side channel.
pub fn thread_scope_presentation(
    thread: &Thread,
    current_principal_id: Option<&PrincipalId>,
    mut capabilities: ThreadPresentationCapabilities,
    authoritative_participants: &[ThreadParticipantSummary],
    scoped_workspace_members: &[MemberSummary],
) -> ThreadScopePresentation {
    let is_user_thread = is_user_thread_origin(thread.origin_kind);
    let is_private = thread.visibility == Some(ThreadVisibility::Private);
    let is_closed = thread.status == ThreadStatus::Closed;
    if !is_user_thread || is_closed {
        capabilities = ThreadPresentationCapabilities::default();
    }

    let participant_ids = authoritative_participants
        .iter()
        .map(|participant| &participant.principal_id)
        .collect::<BTreeSet<_>>();
    let mut participants = Vec::new();
    let mut candidate_members = Vec::new();

    if is_user_thread && is_private {
        for member in scoped_workspace_members {
            if member.status != PrincipalStatus::Active {
                continue;
            }
            let is_current = current_principal_id == Some(&member.principal_id);
            let row = ThreadParticipantRow {
                principal_id: member.principal_id.clone(),
                display_name: member.display_name.clone(),
                nickname: member.nickname.clone(),
                avatar_revision: member.avatar_revision.clone(),
                is_current_principal: is_current,
                can_remove: capabilities.can_manage_private_participants && !is_current,
            };
            if participant_ids.contains(&member.principal_id) {
                participants.push(row);
            } else if capabilities.can_manage_private_participants {
                candidate_members.push(row);
            }
        }
    }

    ThreadScopePresentation {
        visibility: if is_user_thread {
            thread.visibility.into()
        } else {
            ThreadVisibilityPresentation::Unknown
        },
        is_user_thread,
        is_closed,
        capabilities,
        participants,
        candidate_members,
        show_workspace_explanation: is_user_thread
            && thread.visibility == Some(ThreadVisibility::Workspace),
    }
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ThreadScopeAction {
    ListParticipants,
    AddParticipant { principal_id: PrincipalId },
    RemoveParticipant { principal_id: PrincipalId },
    UpdateVisibility { visibility: ThreadVisibility },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "state")]
pub enum ThreadScopePendingAction {
    #[default]
    Idle,
    Pending {
        action: ThreadScopeAction,
    },
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ThreadScopeRefetch {
    Thread,
    Participants,
    WorkspaceMembers,
}

#[cfg_attr(any(feature = "schema", test), derive(schemars::JsonSchema))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ThreadScopeMutationPlan {
    pub workspace_id: WorkspaceId,
    pub thread_id: String,
    pub action: ThreadScopeAction,
    pub refetch: Vec<ThreadScopeRefetch>,
}

pub fn plan_thread_scope_action(
    workspace_id: WorkspaceId,
    thread_id: String,
    action: ThreadScopeAction,
) -> ThreadScopeMutationPlan {
    let refetch = match action {
        ThreadScopeAction::ListParticipants => vec![ThreadScopeRefetch::Participants],
        ThreadScopeAction::AddParticipant { .. } | ThreadScopeAction::RemoveParticipant { .. } => {
            vec![ThreadScopeRefetch::Participants, ThreadScopeRefetch::Thread]
        }
        ThreadScopeAction::UpdateVisibility { .. } => vec![
            ThreadScopeRefetch::Thread,
            ThreadScopeRefetch::Participants,
            ThreadScopeRefetch::WorkspaceMembers,
        ],
    };
    ThreadScopeMutationPlan {
        workspace_id,
        thread_id,
        action,
        refetch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{PrincipalKind, RoleKey, ThreadMode};

    fn create_capabilities() -> AuthorizationWorkspaceCapabilities {
        AuthorizationWorkspaceCapabilities {
            can_read: true,
            can_create_thread: true,
            can_manage: false,
            can_read_own_notifications: true,
            can_acknowledge_own_notifications: true,
            can_use_providers: true,
            can_use_cli_runtimes: true,
            can_use_skills: true,
            can_use_mcp: true,
            can_run_tasks: true,
            can_read_artifacts: true,
            can_write_artifacts: true,
            execution_limits: Default::default(),
            agent_permission_options: Vec::new(),
            can_list_members: true,
            can_add_member: true,
            can_remove_member: false,
            thread_visibility_options: vec![ThreadVisibility::Private, ThreadVisibility::Workspace],
        }
    }

    fn member(id: &str, status: PrincipalStatus) -> MemberSummary {
        MemberSummary {
            principal_id: PrincipalId::new(id).expect("valid principal id"),
            kind: PrincipalKind::User,
            display_name: format!("Member {id}"),
            nickname: id.to_owned(),
            role_key: Some(RoleKey::member()),
            role: pioneer_protocol::AuthorizationRolePresentation {
                key: "member".to_owned(),
                display_name: "Member".to_owned(),
                description: "Workspace collaborator".to_owned(),
                built_in: true,
            },
            lifecycle_managed: true,
            status,
            avatar_revision: None,
        }
    }

    fn thread(origin_kind: ThreadOriginKind, visibility: Option<ThreadVisibility>) -> Thread {
        Thread {
            workspace_id: "workspace_1".into(),
            id: "thread_1".into(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::default(),
            model: String::new(),
            model_provider: String::new(),
            reasoning_effort: None,
            created_at: 0,
            updated_at: 0,
            status: ThreadStatus::Idle,
            origin_kind,
            sidebar_visibility: pioneer_protocol::ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility,
            turns: Vec::new(),
        }
    }

    #[test]
    fn create_options_are_projected_from_workspace_capabilities() {
        let capabilities = create_capabilities();
        let member_plan =
            thread_create_visibility_plan(Some(&capabilities), ThreadOriginKind::User);
        assert_eq!(
            member_plan.default_visibility,
            Some(ThreadVisibility::Private)
        );
        assert_eq!(
            member_plan.options,
            vec![ThreadVisibility::Private, ThreadVisibility::Workspace]
        );

        let mut denied = capabilities.clone();
        denied.can_create_thread = false;
        assert_eq!(
            thread_create_visibility_plan(Some(&denied), ThreadOriginKind::User),
            ThreadCreateVisibilityPlan::default()
        );
        assert_eq!(
            thread_create_visibility_plan(Some(&capabilities), ThreadOriginKind::System),
            ThreadCreateVisibilityPlan::default()
        );
    }

    #[test]
    fn private_projection_uses_only_active_scoped_members() {
        let current = PrincipalId::new("PCCCCCCCCCCCCCCCCCCCC").unwrap();
        let visible = member("PVVVVVVVVVVVVVVVVVVVV", PrincipalStatus::Active);
        let hidden = member("PHHHHHHHHHHHHHHHHHHHH", PrincipalStatus::Suspended);
        let projection = thread_scope_presentation(
            &thread(ThreadOriginKind::User, Some(ThreadVisibility::Private)),
            Some(&current),
            ThreadPresentationCapabilities {
                can_manage_thread: true,
                can_manage_private_participants: true,
                ..ThreadPresentationCapabilities::default()
            },
            &[
                ThreadParticipantSummary {
                    principal_id: visible.principal_id.clone(),
                },
                ThreadParticipantSummary {
                    principal_id: PrincipalId::new("PNNNNNNNNNNNNNNNNNNNN").unwrap(),
                },
            ],
            &[
                visible,
                hidden,
                member("PAAAAAAAAAAAAAAAAAAAA", PrincipalStatus::Active),
            ],
        );
        assert_eq!(projection.participants.len(), 1);
        assert_eq!(projection.candidate_members.len(), 1);
        assert!(projection.capabilities.can_manage_private_participants);
    }

    #[test]
    fn non_creator_and_internal_threads_fail_closed() {
        let projection = thread_scope_presentation(
            &thread(ThreadOriginKind::User, Some(ThreadVisibility::Private)),
            None,
            ThreadPresentationCapabilities::default(),
            &[],
            &[member("PAAAAAAAAAAAAAAAAAAAA", PrincipalStatus::Active)],
        );
        assert!(!projection.capabilities.can_manage_thread);
        assert!(projection.candidate_members.is_empty());

        let internal = thread_scope_presentation(
            &thread(ThreadOriginKind::System, None),
            None,
            ThreadPresentationCapabilities {
                can_manage_thread: true,
                can_manage_private_participants: true,
                ..ThreadPresentationCapabilities::default()
            },
            &[],
            &[],
        );
        assert!(!internal.is_user_thread);
        assert!(!internal.capabilities.can_manage_thread);
        assert_eq!(internal.visibility, ThreadVisibilityPresentation::Unknown);
    }

    #[test]
    fn visibility_transition_plan_refetches_server_owned_explicit_set() {
        let plan = plan_thread_scope_action(
            WorkspaceId::new("WAAAAAAAAAAAAAAAAAAAA").unwrap(),
            "thread_1".into(),
            ThreadScopeAction::UpdateVisibility {
                visibility: ThreadVisibility::Private,
            },
        );
        assert_eq!(
            plan.refetch,
            vec![
                ThreadScopeRefetch::Thread,
                ThreadScopeRefetch::Participants,
                ThreadScopeRefetch::WorkspaceMembers,
            ]
        );
    }
}
